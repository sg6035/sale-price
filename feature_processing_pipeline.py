import os
import warnings
import configparser

import pyspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

from pyspark.ml import Transformer, Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()

configParser = configparser.RawConfigParser()   
configParser.read(r'./configurations.txt')
config_dict = dict(configParser.items('feature_process'))

preprocessed_data_csv = config_dict['preprocessed_data_csv']
feature_scaled_data_csv = config_dict['feature_scaled_data_csv']
feature_scaled_pipeline = config_dict['feature_scaled_pipeline']

class sparseToDense(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which converts sparse to dense vectors
    """
    def __init__(self):
        super(sparseToDense, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:
      sparseToDense = F.udf(lambda v : Vectors.dense(v), VectorUDT())
      densefeatureDF = df.withColumn('features_array', sparseToDense('features'))
      return densefeatureDF

sc= pyspark.SparkContext()
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(preprocessed_data_csv)      

features = ['1stFlrSF', '2ndFlrSF', '3SsnPorch', 'BedroomAbvGr', 'BsmtFinSF1', 'BsmtFinSF2', 'BsmtFullBath', \
                'BsmtHalfBath', 'BsmtUnfSF', 'HalfBath', 'KitchenAbvGr', 'LotFrontage', 'LotArea', 'LowQualFinSF', \
                  'MasVnrArea', 'MiscVal', 'MoSold', 'OpenPorchSF', 'OverallCond', 'OverallQual', 'ScreenPorch', \
                    'PoolArea', 'TotRmsAbvGrd', 'TotalBsmtSF', 'WoodDeckSF', 'YearBuilt', 'YearRemodAdd', 'YrSold', \
                  'EnclosedPorch', 'Fireplaces', 'FullBath',  'GarageCars', 'GarageArea', 'GrLivArea', 'GarageYrBlt']

assembler = VectorAssembler(inputCols= features, outputCol= "features", handleInvalid="keep")
denseTransformer = sparseToDense()
stdscaler = StandardScaler(inputCol= "features_array", outputCol= "scaledfeatures")

stages = [assembler, denseTransformer, stdscaler]

feature_process_pipeline = Pipeline(stages=stages)
pipeline_model_fit = feature_process_pipeline.fit(df)
pipeline_model_fit.write().overwrite().save(feature_scaled_pipeline)

print(os.listdir("/usr/src/app"))