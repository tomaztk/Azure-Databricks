from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
spark = SparkSession.builder.getOrCreate()

# Import
data = spark.read.format("csv").option("header", "true").load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

#Display
display(data)



