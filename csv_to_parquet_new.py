from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

# Initialize SparkSession

conf = SparkConf().setAppName("CrimeAnalysisQ2").set("spark.executor.memory", "2g").set("spark.executor.cores", "2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# List of CSV files
csv_files = ["hdfs:///datasets/crime_data_from_2010_to_2019.csv", "hdfs:///datasets/crime_data_from_2020_to_present.csv",
             "hdfs:///datasets/LA_income_2015.csv", "hdfs:///datasets/LAPD_Police_Stations.csv",
             "hdfs:///datasets/revgecoding.csv"]

# Convert each CSV to Parquet
for csv_file in csv_files:
    # Read CSV file with quote option to handle commas within quotes
    csv_df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .option("quote", '"') \
                       .option("escape", '"') \
                       .csv(csv_file)

    # Extract file name without extension
    file_name = os.path.basename(csv_file).split(".")[0]

    # Write to Parquet with the same name
    parquet_file = f"hdfs:///datasets/parquet_files/{file_name}.parquet"
    csv_df.write.parquet(parquet_file)

# Stop SparkSession
spark.stop()

