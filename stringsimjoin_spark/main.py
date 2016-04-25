from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from tokenizer import *
from match import *

lfile = './test_data/table_A.csv'
rfile = './test_data/table_B.csv'

l_id_attr = 'id'
r_id_attr = 'id'
l_join_attr = 'title'
r_join_attr = 'title'
threshold = 0.8
l_output_attrs = ['id', 'title', 'release']
r_output_attrs = ['id', 'title', 'year']

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

result_table = jaccard_join_spark(sc, ltable, rtable,
                                    l_id_attr, r_id_attr,
                                    l_join_attr, r_join_attr,
                                    tokenizer,
                                    threshold,
                                    l_output_attrs, r_output_attrs)

