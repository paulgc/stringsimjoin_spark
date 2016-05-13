from stringsimjoin_spark.filter.filter_utils import get_prefix_length
from stringsimjoin_spark.index.index import Index
from stringsimjoin_spark.utils.token_ordering import order_using_token_ordering


class PositionIndex(Index):
    def __init__(self, key_attr, index_attr, tokenizer,
                 sim_measure_type, threshold, token_ordering):
        self.key_attr = key_attr
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.sim_measure_type = sim_measure_type
        self.threshold = threshold
        self.token_ordering = token_ordering
        self.index = {}
        self.size_map = {}
        super(self.__class__, self).__init__()

    def build(self, table):
        self.index = table.filter(self.index_attr + ' is not null') \
                            .map(lambda row: (row[self.key_attr],
                                          self.get_position_tokens(
                                              str(row[self.index_attr])))) \
                            .flatMapValues(lambda x: x) \
                            .map(lambda x: (x[1][0], (x[0], x[1][1]))) \
                            .groupByKey() \
                            .mapValues(list) \
                            .collectAsMap()
        self.size_map = table.map(lambda row: (row[self.key_attr],
                                             len(set(self.tokenizer(
                                                 str(row[self.index_attr])))))) \
                            .collectAsMap()
        return True

    def probe(self, token):
        return self.index.get(token, [])

    def get_size(self, row_id):
        return self.size_map.get(row_id)

    def get_position_tokens(self, index_string):
        position_index = []
        index_attr_tokens = order_using_token_ordering(set(
                                        self.tokenizer(index_string)),
                                        self.token_ordering)
        num_tokens = len(index_attr_tokens)
        prefix_length = get_prefix_length(num_tokens,
                                          self.sim_measure_type,
                                          self.threshold)
        pos = 0
        for token in index_attr_tokens[0:prefix_length]:
            position_index.append((token, pos))
            pos += 1
        return position_index
