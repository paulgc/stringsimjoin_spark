from stringsimjoin_spark.index.index import Index


class InvertedIndex(Index):
    def __init__(self, key_attr, index_attr, tokenizer):
        self.key_attr = key_attr
        self.index_attr = index_attr
        self.tokenizer = tokenizer
        self.index = {}
        super(self.__class__, self).__init__()

    def build(self, table):
        self.index = table.filter(self.index_attr + ' is not null') \
                        .map(lambda row: (row[self.key_attr], set(self.tokenizer(row[self.index_attr])))) \
                        .flatMapValues(lambda x: x) \
                        .map(lambda x: (x[1], x[0])) \
                        .groupByKey() \
                        .mapValues(list) \
                        .collectAsMap()

        return True

    def probe(self, token):
        return self.index.get(token, [])
