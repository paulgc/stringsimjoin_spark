from math import floor

from stringsimjoin_spark.filter.filter import Filter
from stringsimjoin_spark.filter.filter_utils import get_overlap_threshold
from stringsimjoin_spark.filter.filter_utils import get_prefix_length

from stringsimjoin_spark.utils.token_ordering import gen_token_ordering_for_lists
from stringsimjoin_spark.utils.token_ordering import order_using_token_ordering


class SuffixFilter(Filter):
    """Suffix filter class.

    Attributes:
        tokenizer: Tokenizer function, which is used to tokenize input string.
        sim_measure_type: String, similarity measure type.
        threshold: float, similarity threshold to be used by the filter.
    """
    def __init__(self, tokenizer, sim_measure_type, threshold):
        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        self.max_depth = 2
        super(self.__class__, self).__init__()

    def filter_pair(self, lstring, rstring):
        """Filter two strings with suffix filter.

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
        return self._filter_suffix(ordered_ltokens[l_prefix_length:],
                             ordered_rtokens[r_prefix_length:],
                             l_prefix_length,
                             r_prefix_length,
                             len(ltokens), len(rtokens))
    
    def _filter_suffix(self, l_ordered_tokens, r_ordered_tokens):

        l_num_tokens = len(l_ordered_tokens)
        r_num_tokens = len(r_ordered_tokens)
        l_prefix_num_tokens = get_prefix_length(l_num_tokens, self.sim_measure_type,
                                            self.threshold)
        r_prefix_num_tokens = get_prefix_length(r_num_tokens, self.sim_measure_type,
                                            self.threshold)
        l_suffix = l_ordered_tokens[l_prefix_num_tokens:]
        r_suffix = r_ordered_tokens[r_prefix_num_tokens:]

        overlap_threshold = get_overlap_threshold(l_num_tokens, r_num_tokens,
                                                  self.sim_measure_type,
                                                  self.threshold)

        hamming_dist_prefix = r_prefix_num_tokens - l_prefix_num_tokens
        if l_num_tokens >= r_num_tokens:
            hamming_dist_prefix = l_prefix_num_tokens - r_prefix_num_tokens
        hamming_dist_max = (l_num_tokens + r_num_tokens -
                            2 * overlap_threshold + hamming_dist_prefix)

        hamming_dist = self._est_hamming_dist_lower_bound(
                                l_suffix, r_suffix,
                                l_num_tokens - l_prefix_num_tokens,
                                r_num_tokens - r_prefix_num_tokens,
                                hamming_dist_max, 1)
        if hamming_dist <= hamming_dist_max:
            return False
        return True


    def _est_hamming_dist_lower_bound(self, l_suffix, r_suffix,
                                      l_suffix_num_tokens,
                                      r_suffix_num_tokens,
                                      hamming_dist_max, depth):
        abs_diff = abs(l_suffix_num_tokens - r_suffix_num_tokens)
        if (depth > self.max_depth or
            l_suffix_num_tokens == 0 or
            r_suffix_num_tokens == 0):
            return abs_diff

        r_mid = int(floor(r_suffix_num_tokens / 2))
        r_mid_token = r_suffix[r_mid]
        o = (hamming_dist_max - abs_diff) / 2

        if l_suffix_num_tokens <= r_suffix_num_tokens:
            o_l = 1
            o_r = 0
        else:
            o_l = 0
            o_r = 1

        (r_l, r_r, flag, diff) = self._partition(
                                  r_suffix, r_mid_token, r_mid, r_mid)
        (l_l, l_r, flag, diff) = self._partition(l_suffix, r_mid_token, 
                                  max(0, int(r_mid - o - abs_diff * o_l)),
                                  min(l_suffix_num_tokens - 1,
                                      int(r_mid + o + abs_diff * o_r)))

        r_l_num_tokens = len(r_l)
        r_r_num_tokens = len(r_r)
        l_l_num_tokens = len(l_l)
        l_r_num_tokens = len(l_r)
        hamming_dist = (abs(l_l_num_tokens - r_l_num_tokens) +
                        abs(l_r_num_tokens - r_r_num_tokens) + diff)

        if hamming_dist > hamming_dist_max:
            return hamming_dist
        else:
            hamming_dist_l = self._est_hamming_dist_lower_bound(
                             l_l, r_l, l_l_num_tokens, r_l_num_tokens,
                             hamming_dist_max -
                                 abs(l_r_num_tokens - r_r_num_tokens) - diff,
                             depth + 1)
            hamming_dist = (hamming_dist_l +
                            abs(l_r_num_tokens - r_r_num_tokens) + diff)
            if hamming_dist <= hamming_dist_max:
                hamming_dist_r = self._est_hamming_dist_lower_bound(
                                 l_r, r_r, l_r_num_tokens, r_r_num_tokens,
                                 hamming_dist_max - hamming_dist_l - diff,
                                 depth + 1)
                return hamming_dist_l + hamming_dist_r + diff
            else:
                return hamming_dist
    
    def _partition(self, tokens, probe_token, left, right):
        right = min(right, len(tokens) - 1)

        if right < left:
            return [], [], 0, 1

        if tokens[left] > probe_token:
            return [], tokens, 0, 1

        if tokens[right] < probe_token:
            return tokens, [], 0, 1

        pos = self._binary_search(tokens, probe_token, left, right)
        tokens_left = tokens[0:pos]

        if tokens[pos] == probe_token:
            tokens_right = tokens[pos+1:len(tokens)]
            diff = 0
        else:
            tokens_right = tokens[pos:len(tokens)]
            diff = 1

        return tokens_left, tokens_right, 1, diff

    def _binary_search(self, tokens, probe_token, left, right):
        if left == right:
            return left

        mid = int(floor((left + right) / 2))
        mid_token = tokens[mid]

        if mid_token == probe_token:
            return mid
        elif mid_token < probe_token:
            return self._binary_search(tokens, probe_token, mid+1, right)
        else:
            return self._binary_search(tokens, probe_token, left, mid)
