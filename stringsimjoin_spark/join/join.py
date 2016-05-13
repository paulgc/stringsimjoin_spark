from pyspark.sql import SQLContext

from stringsimjoin_spark.filter.position_filter import PositionFilter
from stringsimjoin_spark.filter.suffix_filter import SuffixFilter
from stringsimjoin_spark.index.position_index import PositionIndex
from stringsimjoin_spark.utils.helper_functions import \
                                                 get_output_header_from_tables
from stringsimjoin_spark.utils.simfunctions import get_sim_function
from stringsimjoin_spark.utils.token_ordering import gen_token_ordering_for_tables
from stringsimjoin_spark.utils.token_ordering import order_using_token_ordering


def jaccard_join_spark(spark_context,
                 ltable, rtable,
                 l_key_attr, r_key_attr,
                 l_join_attr, r_join_attr,
                 tokenizer,
                 threshold,
                 l_out_attrs=None, r_out_attrs=None,
                 l_out_prefix='l_', r_out_prefix='r_',
                 out_sim_score=True):
    """Join two tables using jaccard similarity measure.

    Finds tuple pairs from ltable and rtable such that
    Jaccard(ltable.l_join_attr, rtable.r_join_attr) >= threshold

    Args:
    ltable, rtable : Spark data frame
    l_key_attr, r_key_attr : String, key attribute from ltable and rtable
    l_join_attr, r_join_attr : String, join attribute from ltable and rtable
    tokenizer : function, tokenizer function to be used to tokenize join attributes
    threshold : float, jaccard threshold to be satisfied
    l_out_attrs, r_out_attrs : list of attributes to be included in the output table from ltable and rtable
    l_out_prefix, r_out_prefix : String, prefix to be used in the attribute names of the output table
    out_sim_score : boolean, indicates if similarity score needs to be included in the output table

    Returns:
    result : Spark data frame
    """
    return sim_join(spark_context,
                    ltable, rtable,
                    l_key_attr, r_key_attr,
                    l_join_attr, r_join_attr,
                    tokenizer,
                    'JACCARD',
                    threshold,
                    l_out_attrs, r_out_attrs,
                    l_out_prefix, r_out_prefix,
                    out_sim_score)


def cosine_join_spark(spark_context,
                ltable, rtable,
                l_key_attr, r_key_attr,
                l_join_attr, r_join_attr,
                tokenizer,
                threshold,
                l_out_attrs=None, r_out_attrs=None,
                l_out_prefix='l_', r_out_prefix='r_',
                out_sim_score=True):
    """Join two tables using cosine similarity measure.

    Finds tuple pairs from ltable and rtable such that
    CosineSimilarity(ltable.l_join_attr, rtable.r_join_attr) >= threshold

    Args:
    ltable, rtable : Spark data frame
    l_key_attr, r_key_attr : String, key attribute from ltable and rtable
    l_join_attr, r_join_attr : String, join attribute from ltable and rtable
    tokenizer : function, tokenizer function to be used to tokenize join attributes
    threshold : float, cosine threshold to be satisfied
    l_out_attrs, r_out_attrs : list of attributes to be included in the output table from ltable and rtable
    l_out_prefix, r_out_prefix : String, prefix to be used in the attribute names of the output table
    out_sim_score : boolean, indicates if similarity score needs to be included in the output table

    Returns:
    result : Spark data frame
    """
    return sim_join(spark_context,
                    ltable, rtable,
                    l_key_attr, r_key_attr,
                    l_join_attr, r_join_attr,
                    tokenizer,
                    'COSINE',
                    threshold,
                    l_out_attrs, r_out_attrs,
                    l_out_prefix, r_out_prefix,
                    out_sim_score)


def sim_join(spark_context,
             ltable, rtable,
             l_key_attr, r_key_attr,
             l_join_attr, r_join_attr,
             tokenizer,
             sim_measure_type,
             threshold,
             l_out_attrs=None, r_out_attrs=None,
             l_out_prefix='l_', r_out_prefix='r_',
             out_sim_score=True):
    """Join two tables using a similarity measure.

    Finds tuple pairs from ltable and rtable such that
    sim_measure(ltable.l_join_attr, rtable.r_join_attr) >= threshold

    Args:
    ltable, rtable : Spark data frame
    l_key_attr, r_key_attr : String, key attribute from ltable and rtable
    l_join_attr, r_join_attr : String, join attribute from ltable and rtable
    tokenizer : function, tokenizer function to be used to tokenize join attributes
    sim_measure_type : String, similarity measure type ('JACCARD', 'COSINE', 'DICE', 'OVERLAP')
    threshold : float, similarity threshold to be satisfied
    l_out_attrs, r_out_attrs : list of attributes to be included in the output table from ltable and rtable
    l_out_prefix, r_out_prefix : String, prefix to be used in the attribute names of the output table
    out_sim_score : boolean, indicates if similarity score needs to be included in the output table

    Returns:
    result : Spark data frame
    """

    # generate token ordering using tokens in l_join_attr
    # and r_join_attr

    token_ordering = gen_token_ordering_for_tables(
                         [ltable,
                          rtable],
                         [l_join_attr,
                          r_join_attr],
                         tokenizer)

    # broadcast token ordering
    token_ordering_bd = spark_context.broadcast(token_ordering.collectAsMap())

    # build a dictionary of l_id mapped to tokenized l_join_attr
    l_join_attr_dict = get_table_dict(ltable, tokenizer, token_ordering_bd, l_key_attr, l_join_attr)

    # broadcast left table id and join attribute
    l_join_attr_dict_bd = spark_context.broadcast(l_join_attr_dict.collectAsMap())

    # Build position index on l_join_attr
    position_index = PositionIndex(l_key_attr, l_join_attr,
                                   tokenizer, sim_measure_type,
                                   threshold, token_ordering_bd.value)
    position_index.build(ltable)

    # broadcast position indices
    position_index_bd = spark_context.broadcast(position_index.index)
    position_index_size_bd = spark_context.broadcast(position_index.size_map)

    pos_filter = PositionFilter(tokenizer, sim_measure_type, threshold)
    suffix_filter = SuffixFilter(tokenizer, sim_measure_type, threshold)
    sim_fn = get_sim_function(sim_measure_type)

    # get right table id mapped to ordered tokens of join attribute
    r_join_attr_dict = get_table_dict(rtable, tokenizer, token_ordering_bd, r_key_attr, r_join_attr)

    candidates = apply_pos_filter(r_join_attr_dict, pos_filter, position_index_bd, position_index_size_bd)

    # populate ordered tokens of left table from broadcasted left table dictionary
    candidates = candidates.map(lambda (r_id, r_ordered_tokens, cand): (r_id, r_ordered_tokens, cand, l_join_attr_dict_bd.value.get(cand))) \

    candidates = apply_suffix_filter(candidates, suffix_filter)

    candidates = apply_sim_function(candidates, sim_fn, threshold)

    candidates = add_unique_cand_id(candidates)

    sql_context = SQLContext(spark_context)
    output_header = get_output_header_from_tables('_id',
                                                  l_key_attr, r_key_attr,
                                                  None, None,
                                                  l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")
    # create a spark dataframe of matching rows ltable id, rtable id
    result_table = sql_context.createDataFrame(candidates, output_header)

    if l_out_attrs:
        # get join of result table with ltable with output columns from left table
        result_table = get_join_table(result_table, ltable, l_out_attrs, l_out_prefix, l_key_attr, output_header[1])

    if r_out_attrs:
        # get join of result table with ltable with output columns from right table
        result_table = get_join_table(result_table, rtable, r_out_attrs, r_out_prefix, r_key_attr, output_header[2])

    output_header = get_output_header_from_tables('_id',
                                                  l_key_attr, r_key_attr,
                                                  l_out_attrs, r_out_attrs,
                                                  l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")
    # reorder column in final output
    return result_table.select(output_header)


def get_table_dict(table, tokenizer, token_ordering_bd, key_attr, join_attr):
    """Get table row id mapped to ordered token of joined attribute.

           Args:
           candidates : Spark dataframe, ltable or rtable
           tokenizer: tokenizer
           token_ordering_bd : Token Order broadcast variable
           key_attr : String, table key attribute
           join_attr : String, table join attribute

           Returns:
           result : Spark RDD, tuples of form (id, ordered_tokens)
           """
    return table.filter(join_attr + ' is not null') \
        .map(lambda row: (row[key_attr], order_using_token_ordering(set(tokenizer(str(row[join_attr]))),
                                                                    token_ordering_bd.value)))


def apply_pos_filter(candidates, pos_filter, position_index_bd, position_index_size_bd):
    """Apply position filter.

        Args:
        candidates : Spark RDD, tuples of form (r_id, r_ordered_tokens)
        pos_filter: position filter
        position_index_bd : Position Index broadcast variable
        position_index_size_bd : Broadcasted sizemap from position index class

        Returns:
        result : Spark RDD, tuples of form (r_id, r_ordered_tokens, cand_l_id)
        """
    return candidates.map(lambda (r_id, r_ordered_tokens): ((r_id, r_ordered_tokens),
                                                            pos_filter._find_candidates(r_ordered_tokens,
                                                                                        position_index_bd.value,
                                                                                        position_index_size_bd.value)))\
        .flatMapValues(lambda x: x.iteritems()) \
        .filter(lambda ((r_id, r_ordered_tokens), (cand, overlap)): overlap > 0) \
        .map(lambda ((r_id, r_ordered_tokens), (cand, overlap)): (r_id, r_ordered_tokens, cand))


def apply_suffix_filter(candidates, suffix_filter):
    """Apply suffix filter. return candidates for which suffix filter returns true

           Args:
           candidates : Spark RDD, tuples of form (r_id, r_ordered_tokens, cand_l_id, l_ordered_tokens)
           suffix_filter: suffix filter

           Returns:
           result : Spark RDD, tuples of form (r_id, r_ordered_tokens, cand_l_id, l_ordered_tokens)
           """
    return candidates.filter(
        lambda (r_id, r_ordered_tokens, cand, l_ordered_tokens): not suffix_filter._filter_suffix(l_ordered_tokens,
                                                                                                  r_ordered_tokens))


def apply_sim_function(candidates, sim_fn, threshold):
    """Apply Similarity measure and filter based on threshold

            Args:
            candidates : Spark RDD, tuples of form (r_id, r_ordered_tokens, cand_l_id, l_ordered_tokens)
            sim_fn: similarity function
            threshold : given threshold

            Returns:
            result : Spark RDD, tuples of form (r_id, l_id, sim_score)
            """
    return candidates.map(lambda (r_id, r_ordered_tokens, cand, l_ordered_tokens): (r_id, r_ordered_tokens,
                                                                                    cand, l_ordered_tokens,
                                                                                    sim_fn(l_ordered_tokens,
                                                                                           r_ordered_tokens))) \
        .filter(lambda (r_id, r_ordered_tokens, cand, l_ordered_tokens, sim_score): sim_score >= threshold) \
        .map(lambda (r_id, r_ordered_tokens, cand, l_ordered_tokens, sim_score): (r_id, cand, sim_score))


def add_unique_cand_id(candidates):
    """Add candidateIds

              Args:
              candidates : Spark RDD, tuples of form (r_id, l_id, sim_score)

              Returns:
              result : Spark RDD, tuples of form (candset_id, l_id, r_id, sim_score)
              """
    return candidates.zipWithUniqueId() \
        .map(lambda ((r_id, cand, sim_score), candset_id): (candset_id, cand, r_id, sim_score))


def get_join_table(result_table, table, out_attrs, out_prefix, key_attr, key_join_attr):
    """Join result table with given table (ltable or rtable).

    Args:
    result_table : Spark data frame
    out_attrs: list of attributes to be included in the output table from table
    out_prefix : String, prefix to be used in the attribute names of the output table
    key_attr : String, key attribute from table
    key_join_attr : String, key join attribute from result_table

    Returns:
    result : Spark data frame, join of result table and given table
    """

    out_attrs_new = [out_prefix + attr for attr in out_attrs]
    # rename columns in table adding prefix to output attributes attributes
    output_table = reduce(lambda table, idx: table.withColumnRenamed(out_attrs[idx], out_attrs_new[idx]),
                          xrange(len(out_attrs)), table)
    # select output attributes from table and then join it with result table
    join_table = output_table.select(out_attrs_new + [key_attr]).join(result_table,
                                                                      output_table[key_attr] == result_table[
                                                                          key_join_attr]).drop(key_attr)
    return join_table
