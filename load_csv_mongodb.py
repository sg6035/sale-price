import configparser
import pandas as pd
from pymongo import MongoClient

configParser = configparser.RawConfigParser()   
configParser.read('./load_csv_mongodb_config.txt')
config_dict = dict(configParser.items('load_csv_mongodb'))

mongo_client = config_dict['mongo_client']
test_file_name = config_dict['test_file_name']
test_col_name = config_dict['test_col_name']
train_file_name = config_dict['train_file_name']
train_col_name =  config_dict['train_col_name']

def loadCSVToMongo(file_name, col_name):
    df = pd.read_csv(file_name)
    data = df.to_dict('records')    
    col = db[col_name]
    col.insert_many(data)

client = MongoClient(mongo_client)

db_name = config_dict['database_name']
db = client[db_name]

loadCSVToMongo(test_file_name, test_col_name)
loadCSVToMongo(train_file_name, train_col_name)