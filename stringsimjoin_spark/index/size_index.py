from stringsimjoin_spark.index.index import Index


class SizeIndex(Index):
    def __init__(self, key_attr, index_attr, tokenizer):
        self.key_attr = key_attr
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.index = {}
        self.min_length = 0
        self.max_length = 0
        super(self.__class__, self).__init__()

    def build(self, table):
        self.index = table.filter(self.index_attr + ' is not null') \
                        .map(lambda row: (len(set(self.tokenizer(row[self.index_attr]))), row[self.key_attr])) \
                        .groupByKey() \
                        .mapValues(list)
        self.min_length = self.index.min(lambda x: x[0])
        self.max_length = self.index.max(lambda x: x[0])

        return True

    def probe(self, num_tokens):
        return self.index.get(num_tokens, [])
