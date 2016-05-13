from operator import add

def gen_token_ordering_for_lists(token_lists):
    token_freq_dict = {}
    for token_list in token_lists:
        for token in token_list:
            token_freq_dict[token] = token_freq_dict.get(token, 0) + 1
            order_idx = 1

    token_ordering = {}
    for token in sorted(token_freq_dict, key=token_freq_dict.get):
        token_ordering[token] = order_idx
        order_idx += 1

    return token_ordering


def gen_token_ordering_for_tables(table_list, attr_list, tokenizer):
    token_freq_dict = None
    table_index = 0
    for table in table_list:
        freq_dict = table.flatMap(lambda row: set(
                                tokenizer(str(row[attr_list[table_index]])))) \
                            .map(lambda token: (token, 1)) \
                            .reduceByKey(add)
        if token_freq_dict:
            token_freq_dict = token_freq_dict.union(freq_dict).reduceByKey(add)
        else:
            token_freq_dict = freq_dict
        table_index += 1

    token_ordering = token_freq_dict.sortBy(lambda (token, freq): freq) \
            .map(lambda (token, freq): token) \
            .zipWithIndex()
    return token_ordering


def order_using_token_ordering(tokens, token_ordering):
    ordered_tokens = []

    for token in tokens:
        order = token_ordering.get(token)
        if order is not None:
            ordered_tokens.append(order)

    ordered_tokens.sort()

    return ordered_tokens
