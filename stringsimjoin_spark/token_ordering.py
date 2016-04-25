from operator import add

def gen_token_ordering(tokenized_table, attr):
    token_ordering = tokenized_table.flatMap(lambda x: set(x[attr])) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add) \
              .sortBy(lambda x: x[1])
    return token_ordering


def order_using_token_ordering(tokens, token_ordering):
    tokens_dict = {}
    new_tokens = []
    for token in tokens:
        order_index = token_ordering.value.get(token)
        if order_index != None:
            tokens_dict[token] = order_index
        else:
            new_tokens.append(token)
    ordered_tokens = []
    for token in sorted(tokens_dict, key=tokens_dict.get):
        ordered_tokens.append(token)
    for token in new_tokens:
        ordered_tokens.append(token)
    return ordered_tokens
