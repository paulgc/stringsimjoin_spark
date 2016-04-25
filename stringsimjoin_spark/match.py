from operator import add
from tokenizer import *
from token_ordering import *
from filter_utils import *
from sim_utils import *
from pyspark.sql import SQLContext



def jaccard_join_spark(sc, A, B, 
                          l_id_attr, r_id_attr,
                          l_join_attr, r_join_attr,
                          tokenizer,
                          threshold,
                          l_output_attrs = None, r_output_attrs = None,
                          l_output_prefix = 'ltable', r_output_prefix = 'rtable',
                          output_sim_score = False):

    sql_context = SQLContext(sc)
    tokens_attr = 'tokens'
    # Tokenize tables: add tokens column to table
    l_tokenized_table = tokenize_table(A, l_join_attr, tokenizer, tokens_attr)
    r_tokenized_table = tokenize_table(B, r_join_attr, tokenizer, tokens_attr)


    # Generate token ordering from left table based on frequency and broadcast the token order
    token_ordering = gen_token_ordering(l_tokenized_table, tokens_attr)
    token_order_bd = sc.broadcast(token_ordering.collectAsMap())


    # Create size filter index and broadcast it
    size_index = get_size_index(r_tokenized_table, r_id_attr, tokens_attr)
    size_index_bd = sc.broadcast(size_index.collectAsMap())


    # Create prefix filter index and broadcast it
    prefix_index = get_prefix_index(r_tokenized_table, r_id_attr, tokens_attr, token_order_bd, threshold)
    prefix_index_bd = sc.broadcast(prefix_index.collectAsMap())


    candidates = l_tokenized_table.map(lambda x : (x[l_id_attr], x[tokens_attr], find_candidates(x[tokens_attr], size_index_bd, prefix_index_bd, threshold)))


    # Create map from row id to row tokenized join attr and broadcast it
    r_id_tokens = r_tokenized_table.map(lambda x : (x[r_id_attr], x[tokens_attr]))
    r_id_tokens_bd = sc.broadcast(r_id_tokens.collectAsMap())



    sim_function = get_jaccard_fn()

    result = candidates.map(lambda x : (x[0], get_sim_score(x[0], x[1], x[2], r_id_tokens_bd, sim_function))) \
                        .flatMapValues(lambda x : x) \
			.filter(lambda x : x[1][1] > threshold) \
                        .map(lambda x : (x[0], x[1][0], x[1][1]))


    # Create result dataframe with left table id, right table id and sim score
    result_schema = ['l_id', 'r_id', 'sim_score']
    result = sql_context.createDataFrame(result, result_schema)


    # Join left table with result based on left table id
    joined_table = l_tokenized_table.join(result, l_tokenized_table[l_id_attr] == result['l_id']).select(l_output_attrs + result_schema)


    # Join resulted table with right table on right table id
    output_attrs = [joined_table[attr] for attr in l_output_attrs + ['sim_score']] + [r_tokenized_table[attr] for attr in r_output_attrs]
    result_table = joined_table.join(r_tokenized_table, joined_table['r_id'] == r_tokenized_table[r_id_attr]).select(*output_attrs)

    return result_table
