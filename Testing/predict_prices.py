import os
import warnings
import configparser

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, mean
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.ml import Transformer, PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.param.shared import HasOutputCols
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

from pyspark.ml.linalg import Vectors, VectorUDT

warnings.filterwarnings("ignore")

#define mode function
def modeFunction(df, column):
  return df.groupby(column).count().orderBy("count", ascending=False).first()[0]

def dataType(df, c):
  return df.select(c).dtypes[0][1]

######################################################################################
#####     DEFINED ALL TRANSFORMER CLASSES REQUIRED INTERNALLY BY PIPELINES HERE.
#####     WITHOUT THIS DEFINITIONS PIPELINES ARE FAILING.
######################################################################################

# Writing a custom transformer
class imputeNA_mode(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which imputes NA with mode values in the column.
    """
    def __init__(self):
        super(imputeNA_mode, self).__init__()
        columns_to_imputeNA = [ "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", \
          "BsmtFinType2", "Electrical", "GarageType", "GarageFinish", "GarageQual", "GarageCond"]
        self.imp_col = columns_to_imputeNA     
    
    def _transform(self, df: DataFrame) -> DataFrame:
      for c in self.imp_col:
        df = df.withColumn(c, when(df[c] == "NA", modeFunction(df, c)).otherwise(df[c]))
      return df

# Writing a custom transformer
class convertToInt(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which imputes NA with mode values in the column.
    """
    def __init__(self):
        super(convertToInt, self).__init__()
        columns_to_int = ["LotFrontage", "ExterQual", "ExterCond", "BsmtQual", 
                    "BsmtCond", "HeatingQC", "KitchenQual", "FireplaceQu", "GarageQual", "GarageCond", 
                    "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Functional", "GarageFinish", "Fence", "GarageYrBlt" ]
        self.int_cols = columns_to_int     
    
    def _transform(self, df: DataFrame) -> DataFrame:
      for c in self.int_cols:
        df = df.withColumn(c, col(c).cast(IntegerType()))
      return df

# Writing a custom transformer to replace NA in Garage built year with remodeled year
class imputeAllColumns(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which imputes all the required values with appropriate values.
    """
    def __init__(self):
        super(imputeAllColumns, self).__init__()
        
    def _transform(self, df: DataFrame) -> DataFrame:
      df = df.withColumn("GarageYrBlt", when(df["GarageYrBlt"] == "NA", df["YearRemodAdd"]).otherwise(df["GarageYrBlt"]))

      #Replace 0 in GarageCars with mode value
      df = df.withColumn("GarageCars", when(df["GarageCars"] == 0, modeFunction(df, 'GarageCars')).otherwise(df["GarageCars"]))

      #Replace 0 with mean in GarageArea
      m1 = df.select(mean("GarageArea")).collect()[0][0]
      df = df.withColumn("GarageArea", when(df["GarageArea"] == 0, m1).otherwise(df["GarageArea"]))

      #Replacing NA values in LotFrontage with mean value
      m2 = df.select(mean("LotFrontage")).collect()[0][0]
      df = df.withColumn("LotFrontage", when(df["LotFrontage"] == "NA", m2).otherwise(df["LotFrontage"]))

      #Replace NA n MasVnrArea wth mean
      m3 = df.select(mean("MasVnrArea")).collect()[0][0]
      df = df.withColumn("MasVnrArea", when(df["MasVnrArea"] == "NA", m3).otherwise(df["MasVnrArea"]))
      return df

class replaceQualityValues(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which replaces all the quality values with appropriate numeric values.
    """
    def __init__(self):
        super(replaceQualityValues, self).__init__()
        
    def _transform(self, df: DataFrame) -> DataFrame:
      qualityIntegers = {"NA": 0, "Po": 1, "Fa": 2, "TA": 3, "Gd": 4, "Ex": 5}
      qualityColumns = ["ExterQual", "ExterCond", "BsmtQual", "BsmtCond", "HeatingQC", "KitchenQual", "FireplaceQu", "GarageQual", "GarageCond"] 
      for c in qualityColumns:
        for k, v in qualityIntegers.items():
          df = df.withColumn(c, when(df[c] == k, v).otherwise(df[c]))

      ExposureIntegers = {"NA": 0, "No": 1, "Mn": 2, "Av": 3, "Gd": 4}
      for k, v in ExposureIntegers.items():
        df = df.withColumn("BsmtExposure", when(df["BsmtExposure"] == k, v).otherwise(df["BsmtExposure"]))

      FinIntegers = {"NA": 0, "Unf": 1, "LwQ": 2, "Rec": 3, "BLQ": 4, "ALQ": 5, "GLQ": 6}
      FinCols = ["BsmtFinType1", "BsmtFinType2"]
      for c in FinCols:
        for k, v in FinIntegers.items():
          df = df.withColumn(c, when(df[c] == k, v).otherwise(df[c]))

      FunctionalIntegers = {"NA": 0, "Sal": 1, "Sev": 2, "Maj2": 3, "Maj1": 4, "Mod": 5, "Min2": 6, "Min1": 7, "Typ": 8}
      for k, v in FunctionalIntegers.items():
        df = df.withColumn("Functional", when(df["Functional"] == k, v).otherwise(df["Functional"]))

      GFinIntegers = {"NA": 0, "Unf": 1, "RFn": 2, "Fin": 3}
      for k, v in GFinIntegers.items():
        df = df.withColumn("GarageFinish", when(df["GarageFinish"] == k, v).otherwise(df["GarageFinish"]))

      FenceIntegers = {"NA": 0, "MnWw": 1, "GdWo": 2, "MnPrv": 3, "GdPrv": 4}
      for k, v in FenceIntegers.items():
        df = df.withColumn("Fence", when(df["Fence"] == k, v).otherwise(df["Fence"]))

      return df

class oneHotEncoder(Transformer, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer which imputes all the required values with appropriate values.
    """
    def __init__(self):
        super(oneHotEncoder, self).__init__()
        
    def _transform(self, df: DataFrame) -> DataFrame:
      for c in df.columns:
        if dataType(df, c) == 'string':
          newC = "_" + c
          indexer = StringIndexer(inputCol=c, outputCol=newC)
          df = indexer.fit(df).transform(df)
          df = df.drop(c)
          df = df.withColumnRenamed(newC, c)
          df = df.withColumn(c, col(c).cast(IntegerType()))
      return df

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

######################################################################################
#####    ACTUAL TEST CODE STARTS FROM HERE.!
######################################################################################

configParser = configparser.RawConfigParser()   
configParser.read(r'./test_configurations.txt')
config_dict = dict(configParser.items('predict'))

data_csv = config_dict['data_csv']
preprocessed_data_csv = config_dict['preprocessed_data_csv']
data_preprocess_pipeline_path = config_dict['data_preprocess_pipeline']
feature_scaled_pipeline_path = config_dict['feature_scaled_pipeline']
predicted_test_data_csv = config_dict['predicted_test_data_csv']
reg_model_path = config_dict['reg_model_path']

sc= pyspark.SparkContext()
spark = SparkSession.builder. \
        config('spark.sql.codegen.wholeStage', 'false'). \
        getOrCreate()

df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(data_csv)

#Load all pipelines
preprocess_model = PipelineModel.load(data_preprocess_pipeline_path)
feature_process_model = PipelineModel.load(feature_scaled_pipeline_path)
reg_model = PipelineModel.load(reg_model_path)

#Transform data with all the fitted models
transformed_data = preprocess_model.transform(df)
scaled_df = feature_process_model.transform(transformed_data)
data = scaled_df.select(F.col("Id"), F.col("scaledfeatures").alias("features"))
predictionDF = reg_model.transform(data)

p_df = predictionDF.toPandas()
p_df.drop('features', axis = 1, inplace=True)
p_df.to_csv(predicted_test_data_csv, index=False)
print(p_df)