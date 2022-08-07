import os
import warnings
import configparser

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import PipelineModel, Pipeline
from pyspark.sql import functions as F

from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Transformer, PipelineModel

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import  GBTRegressor

warnings.filterwarnings("ignore")

def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()

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


configParser = configparser.RawConfigParser()   
configParser.read(r'./configurations.txt')
config_dict = dict(configParser.items('feature_process'))

preprocessed_data_csv = config_dict['preprocessed_data_csv']
feature_scaled_pipeline_path = config_dict['feature_scaled_pipeline']
predicted_data_csv = config_dict['predicted_data_csv']
reg_model_path = config_dict['reg_model_path']

sc= pyspark.SparkContext()
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(preprocessed_data_csv)

feature_process_model = PipelineModel.load(feature_scaled_pipeline_path)
scaled_df = feature_process_model.transform(df)

data = scaled_df.select(
    F.col("scaledfeatures").alias("features"),
    F.col("SalePrice").alias("label"),
)

trainDF, testDF =  data.randomSplit([0.8, 0.2], seed = 22)

regression_model = GBTRegressor(maxDepth=3, maxIter=100)

reg_pipe = Pipeline(stages = [regression_model])
reg_model_fit = reg_pipe.fit(trainDF)
reg_model_fit.write().overwrite().save(reg_model_path)

predictionDF = reg_model_fit.transform(testDF)

p_df = predictionDF.toPandas()
p_df.to_csv(predicted_data_csv, index=False)
