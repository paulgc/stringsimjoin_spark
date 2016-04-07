from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

def tokenize_table(table, attr, tokenizer, tokens_attr):
    udfTokenizer = udf(tokenizer, ArrayType(StringType()))
    tokenized_table = table.withColumn(tokens_attr, udfTokenizer(attr))
    return tokenized_table


def get_delim_tokenizer(delim):
    def tok_delim(s):
        return s.split(delim)
    return tok_delim
