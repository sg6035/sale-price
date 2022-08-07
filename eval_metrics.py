import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

sc= pyspark.SparkContext()
spark = SparkSession.builder.getOrCreate()
predictionDF = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/usr/src/app/predicted_data.csv")

rmse_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
rmse = rmse_evaluator.evaluate(predictionDF)
print("RMSE: " + str(rmse))

r2_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="r2")
r2 = r2_evaluator.evaluate(predictionDF)
print("R2 Score: " + str(r2))