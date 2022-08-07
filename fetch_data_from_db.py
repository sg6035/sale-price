import os
import configparser

import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Fetch required configuration values from a config file
configParser = configparser.RawConfigParser()   
configParser.read(r'./configurations.txt')
config_dict = dict(configParser.items('fetch_train_data'))

mongo_uri = config_dict['mongo_uri']
database_name = config_dict['database_name']
train_collection_name = config_dict['train_collection_name']
data_csv = config_dict['data_csv']

sc= pyspark.SparkContext()
spark = SparkSession.builder. \
    config("spark.mongodb.input.uri", mongo_uri). \
    getOrCreate()

train_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("database", database_name). \
        option("collection", train_collection_name). \
        option("uri", mongo_uri).load()

p_df = train_df.toPandas()
p_df.to_csv(data_csv, index=False)

print(os.getcwd())
print(os.listdir("/usr/src/app"))