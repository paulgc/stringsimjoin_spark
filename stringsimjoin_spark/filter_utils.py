from math import ceil, floor
from token_ordering import *


def get_size_index(tokenized_table, id_attr, join_attr):
    index = tokenized_table.map(lambda x : (len(set(x[join_attr])), x[id_attr])) \
                    .groupByKey() \
                    .mapValues(list)
    return index


def find_size_candidates(size_index, tokens, threshold):
    num_tokens = len(set(tokens))
    start_length = int(floor(threshold*num_tokens))
    end_length = int(ceil(num_tokens/threshold))
    candidates = set()
    for i in xrange(start_length, end_length+1):
        candidates_for_i = size_index.value.get(i)
        if candidates_for_i is not None:
            for cand in candidates_for_i:
                candidates.add(cand)
    return candidates



def get_prefix(tokens, token_ordering, threshold):
    prefix_index = {}
    token_list = list(tokens)
    ordered_token_list = order_using_token_ordering(token_list, token_ordering)
    num_tokens = len(ordered_token_list)
    prefix_length = int(num_tokens - ceil(threshold * num_tokens) + 1)
    i = 0
    for token in ordered_token_list:
        if i == prefix_length:
            break
        if prefix_index.get(token) is None:
            prefix_index[token] = []
    return prefix_index            


def get_prefix_index(tokenized_table, id_attr, join_attr, token_ordering, threshold):
    
    index = tokenized_table.map(lambda x : (x[id_attr], get_prefix(x[join_attr], token_ordering, threshold))) \
            .flatMapValues(lambda x: x) \
            .map(lambda x : (x[1], x[0])) \
            .groupByKey() \
            .mapValues(list)
    return index                       


def find_prefix_candidates(prefix_index, probe_tokens, threshold):
    num_tokens = len(probe_tokens)
    candidates = set()
    prefix_length = int(num_tokens - ceil(threshold * num_tokens) + 1)
    i = 0
    for token in probe_tokens:
        if i == prefix_length:
            break
        candidates_for_token = prefix_index.value.get(token)
        if candidates_for_token is not None:
            for cand in candidates_for_token:
                candidates.add(cand)
    return candidates



def find_candidates(tokens, size_index_bd, prefix_index_bd, threshold):
    candidates = set()
    candidates = find_size_candidates(size_index_bd, tokens, threshold)
    candidates.intersection_update(find_prefix_candidates(prefix_index_bd, tokens, threshold))
    return candidates

