from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.sql import SQLContext
from tokenizer import *
from token_ordering import *
from filter_utils import *
from sim_utils import *

lfile = './test_data/table_A.csv'
rfile = './test_data/table_B.csv'

l_id_attr = 'id'
r_id_attr = 'id'
l_join_attr = 'title'
r_join_attr = 'title'
tokens_attr = 'tokens'
threshold = 0.8

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("String Sim Join")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Load csv files into tables
ltable = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(lfile)
rtable = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(rfile)


# Tokenize tables: add tokens column to table
tokenizer = get_delim_tokenizer(' ')
l_tokenized_table = tokenize_table(ltable, l_join_attr, tokenizer, tokens_attr)
r_tokenized_table = tokenize_table(rtable, r_join_attr, tokenizer, tokens_attr)


# Generate token ordering from left table based on frequency and broadcast the token order
token_ordering = gen_token_ordering(l_tokenized_table, tokens_attr)
token_order_bd = sc.broadcast(token_ordering.collectAsMap())


# Create size filter index and broadcast it
size_index = get_size_index(r_tokenized_table, r_id_attr, tokens_attr)
size_index_bd = sc.broadcast(size_index.collectAsMap())


# Create prefix filter index and broadcast it
prefix_index = get_prefix_index(r_tokenized_table, r_id_attr, tokens_attr, token_order_bd, threshold)
prefix_index_bd = sc.broadcast(prefix_index.collectAsMap())


candidates = l_tokenized_table.map(lambda x : (x[l_id_attr], find_candidates(x[tokens_attr], size_index_bd, prefix_index_bd, threshold)))

candidates_bd = sc.broadcast(candidates.collectAsMap())

prefix_index_bd.unpersist()
size_index_bd.unpersist()
token_order_bd.unpersist()

