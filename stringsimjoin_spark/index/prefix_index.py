from stringsimjoin_spark.filter.filter_utils import get_prefix_length
from stringsimjoin_spark.index.index import Index
from stringsimjoin_spark.utils.token_ordering import order_using_token_ordering


class PrefixIndex(Index):
    def __init__(self, key_attr, index_attr, tokenizer,
                 sim_measure_type, threshold, token_ordering):
        self.key_attr = key_attr
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.index = {}
        super(self.__class__, self).__init__()

    def build(self, table):
        self.index = table.filter(self.index_attr + ' is not null') \
                        .map(lambda row: (row[self.key_attr], self.get_prefix_tokens(str(row[self.index_attr])))) \
                        .flatMapValues(lambda x: x) \
                        .map(lambda x: (x[1], x[0])) \
                        .groupByKey() \
                        .mapValues(list) \
                        .collectAsMap()
        return True

    def probe(self, token):
        return self.index.get(token, [])

    def get_prefix_tokens(self, index_string):
        index_attr_tokens = order_using_token_ordering(set(
            self.tokenizer(index_string)),
            self.token_ordering)
        prefix_length = get_prefix_length(
            len(index_attr_tokens),
            self.sim_measure_type, self.threshold)

        prefix_tokens = index_attr_tokens[:prefix_length]
        return prefix_tokens