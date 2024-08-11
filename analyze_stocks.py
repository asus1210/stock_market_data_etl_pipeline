"""Analyze Stock Market Data"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType,DoubleType, LongType

PATH_CSV_FILES = "./data/stocks/AAPL.csv"

#spark session
spark = SparkSession.builder.appName("Stock_Market_Data_Analysis").getOrCreate()


# Load all CSV files with the defined schema
df = spark.read.csv("./data/cleaned_stocks/AAPL.csv", header=True, inferSchema = True)
print("Done Reading")

# # Convert Volume column to LongType
# df_with_integer_volume = df.withColumn("Volume", col("Volume").cast(LongType()))

# df_with_filename = df_with_integer_volume.withColumn("filename", regexp_extract(input_file_name(), ".*/(.*)", 1))
# print("Done")

# Show the schema and some rows to verify
df.printSchema()
df.show()
