from stringsimjoin_spark.filter.filter import Filter
from stringsimjoin_spark.filter.filter_utils import get_overlap_threshold
from stringsimjoin_spark.filter.filter_utils import get_prefix_length
from stringsimjoin_spark.filter.filter_utils import get_size_lower_bound
from stringsimjoin_spark.filter.filter_utils import get_size_upper_bound

from stringsimjoin_spark.utils.token_ordering import gen_token_ordering_for_lists
from stringsimjoin_spark.utils.token_ordering import order_using_token_ordering


class PositionFilter(Filter):
    """Position filter class.

    Attributes:
        tokenizer: Tokenizer function, which is used to tokenize input string.
        sim_measure_type: String, similarity measure type.
        threshold: float, similarity threshold to be used by the filter.
    """
    def __init__(self, tokenizer, sim_measure_type, threshold):
        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        super(self.__class__, self).__init__()

    def filter_pair(self, lstring, rstring):
        """Filter two strings with position filter.

        Args:
        lstring, rstring : input strings

        Returns:
        result : boolean, True if the tuple pair is dropped.
        """
        # check for empty string
        if (not lstring) or (not rstring):
            return True

        ltokens = list(set(self.tokenizer(lstring)))
        rtokens = list(set(self.tokenizer(rstring)))

        token_ordering = gen_token_ordering_for_lists([ltokens, rtokens])
        ordered_ltokens = order_using_token_ordering(ltokens, token_ordering)
        ordered_rtokens = order_using_token_ordering(rtokens, token_ordering)

        l_num_tokens = len(ordered_ltokens)
        r_num_tokens = len(ordered_rtokens)

        l_prefix_length = get_prefix_length(l_num_tokens,
                                            self.sim_measure_type,
                                            self.threshold) 
        r_prefix_length = get_prefix_length(r_num_tokens,
                                            self.sim_measure_type,
                                            self.threshold)
 
        l_prefix_dict = {}
        l_pos = 0
        for token in ordered_ltokens[0:l_prefix_length]:
            l_prefix_dict[token] = l_pos

        overlap_threshold = get_overlap_threshold(l_num_tokens, r_num_tokens,
                                                  self.sim_measure_type,
                                                  self.threshold)
        current_overlap = 0
        r_pos = 0 
        for token in ordered_rtokens[0:r_prefix_length]:
            l_pos = l_prefix_dict.get(token)
            if l_pos is not None:
                overlap_upper_bound = 1 + min(l_num_tokens - l_pos - 1,
                                              r_num_tokens - r_pos - 1)
                if (current_overlap + overlap_upper_bound) < overlap_threshold:
                    return True
                current_overlap += 1
            r_pos += 1

        if current_overlap > 0:
            return False
        return True


    def _find_candidates(self, r_ordered_tokens,
                         position_index, position_index_size):
        r_num_tokens = len(r_ordered_tokens)
        r_prefix_length = get_prefix_length(r_num_tokens, self.sim_measure_type,
                                            self.threshold)
        size_lower_bound = get_size_lower_bound(r_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold)
        size_upper_bound = get_size_upper_bound(r_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold)

        overlap_threshold_cache = {}
        for size in xrange(size_lower_bound, size_upper_bound + 1):
            overlap_threshold_cache[size] = get_overlap_threshold(
                                                size, r_num_tokens,
                                                self.sim_measure_type,
                                                self.threshold)

        # probe position index and find candidates
        candidate_overlap = {}
        r_pos = 0
        for token in r_ordered_tokens[0:r_prefix_length]:
            for (cand, cand_pos)  in position_index.get(token):
                cand_num_tokens = position_index_size.get(cand)
                if size_lower_bound <= cand_num_tokens <= size_upper_bound:
                    overlap_upper_bound = 1 + min(r_num_tokens - r_pos - 1,
                                              cand_num_tokens - cand_pos - 1)
                    current_overlap = candidate_overlap.get(cand, 0)
                    if (current_overlap + overlap_upper_bound >=
                            overlap_threshold_cache[cand_num_tokens]):
                        candidate_overlap[cand] = current_overlap + 1
                    else:
                        candidate_overlap[cand] = 0
            r_pos += 1

        return candidate_overlap
