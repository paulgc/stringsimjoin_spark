

def get_sim_score(l_id, l_tokens, r_id_list, r_id_tokens_bd, sim_function):
    sim_score = []
    for r_id in r_id_list:
        r_tokens = r_id_tokens_bd.value.get(r_id)
        sim = sim_function(set(l_tokens), set(r_tokens))
        sim_score.append((r_id, sim))
    return sim_score


def get_jaccard_fn():
    return jaccard

def jaccard(set1, set2):
    """
    Computes the Jaccard measure between two sets.
    The Jaccard measure, also known as the Jaccard similarity coefficient, is a statistic used for comparing
    the similarity and diversity of sample sets. The Jaccard coefficient measures similarity between finite sample
    sets, and is defined as the size of the intersection divided by the size of the union of the sample sets.
    For two sets X and Y, the Jaccard measure is:
    :math:`jaccard(X, Y) = \\frac{|X \\cap Y|}{|X| \\cup |Y|}`
    Args:
        set1,set2 (set or list): Input sets (or lists). Input lists are converted to sets.
    Returns:
        Jaccard similarity (float)
    Raises:
        TypeError : If the inputs are not sets (or lists).
    Examples:
        >>> jaccard(['data', 'science'], ['data'])
        0.5
        >>> jaccard({1, 1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 7, 8})
        0.375
        >>> jaccard(['data', 'management'], ['data', 'data', 'science'])
        0.3333333333333333
    """

#    if not isinstance(set1, set):
#        set1 = set(set1)
#    if not isinstance(set2, set):
#        set2 = set(set2)
    return float(len(set1 & set2)) / float(len(set1 | set2))
