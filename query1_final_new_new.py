from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, count, row_number
from pyspark.sql.window import Window
import time

# Initialize Spark session
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# Function to run query using DataFrame API
def run_query_dataframe(file_paths, file_format):
    if file_format == "csv":
        df1 = spark.read.format(file_format) \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .load(file_paths[0])
        df2 = spark.read.format(file_format) \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .load(file_paths[1])
        df = df1.union(df2)
    else:
        df1 = spark.read.parquet(file_paths[0])
        df2 = spark.read.parquet(file_paths[1])
        df = df1.union(df2)

    crime_df = df.withColumn('Year', substring('DATE OCC', 7, 4).cast('int')) \
                 .withColumn('Month', substring('DATE OCC', 1, 2).cast('int'))
    crime_counts = crime_df.groupBy('Year', 'Month').agg(count('*').alias('crime_total'))
    window_spec = Window.partitionBy('Year').orderBy(col('crime_total').desc())
    ranked_crime_counts = crime_counts.withColumn('ranking', row_number().over(window_spec))
    top_months_per_year = ranked_crime_counts.filter(col('ranking') <= 3)
    return top_months_per_year.orderBy('Year', 'ranking')

# Function to run query using SQL API
def run_query_sql(file_paths, file_format):
    if file_format == "csv":
        df1 = spark.read.format(file_format) \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .load(file_paths[0])
        df2 = spark.read.format(file_format) \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .load(file_paths[1])
        df = df1.union(df2)
    else:
        df1 = spark.read.parquet(file_paths[0])
        df2 = spark.read.parquet(file_paths[1])
        df = df1.union(df2)

    df.createOrReplaceTempView("crime_data")
    query = """
    SELECT Year, Month, crime_total, ranking FROM (
        SELECT
            substring(`DATE OCC`, 7, 4) as Year,
            substring(`DATE OCC`, 1, 2) as Month,
            count(*) as crime_total,
            RANK() OVER (PARTITION BY substring(`DATE OCC`, 7, 4) ORDER BY count(*) DESC) as ranking
        FROM crime_data
        GROUP BY substring(`DATE OCC`, 7, 4), substring(`DATE OCC`, 1, 2)
    ) t
    WHERE ranking <= 3
    ORDER BY Year, ranking
    """
    results = spark.sql(query)
    return results

# Define file paths
csv_file_paths = [
    'hdfs:///datasets/crime_data_from_2010_to_2019.csv',
    'hdfs:///datasets/crime_data_from_2020_to_present.csv'
]
parquet_file_paths = [
    'hdfs:///datasets/parquet_files/crime_data_from_2010_to_2019.parquet',
    'hdfs:///datasets/parquet_files/crime_data_from_2020_to_present.parquet'
]

# Measure execution time for DataFrame API with CSV
start_time = time.time()
results_df_csv = run_query_dataframe(csv_file_paths, "csv")
end_time = time.time()
df_csv_time = end_time - start_time

# Measure execution time for DataFrame API with Parquet
start_time = time.time()
results_df_parquet = run_query_dataframe(parquet_file_paths, "parquet")
end_time = time.time()
df_parquet_time = end_time - start_time

# Measure execution time for SQL API with CSV
start_time = time.time()
results_sql_csv = run_query_sql(csv_file_paths, "csv")
end_time = time.time()
sql_csv_time = end_time - start_time

# Measure execution time for SQL API with Parquet
start_time = time.time()
results_sql_parquet = run_query_sql(parquet_file_paths, "parquet")
end_time = time.time()
sql_parquet_time = end_time - start_time

# Print results and execution times
print("DataFrame API Results (CSV):")
results_df_csv.show(truncate=False, n=results_df_csv.count())

print("DataFrame API Execution Time (CSV):", df_csv_time)

print("DataFrame API Results (Parquet):")
results_df_parquet.show(truncate=False, n=results_df_parquet.count())

print("DataFrame API Execution Time (Parquet):", df_parquet_time)

print("SQL API Results (CSV):")
results_sql_csv.show(truncate=False, n=results_sql_csv.count())

print("SQL API Execution Time (CSV):", sql_csv_time)

print("SQL API Results (Parquet):")
results_sql_parquet.show(truncate=False, n=results_sql_parquet.count())

print("SQL API Execution Time (Parquet):", sql_parquet_time)

# Present results in a table format
results_table = f"""
| Implementation   | File Format | Execution Time (seconds) |
|------------------|-------------|--------------------------|
| DataFrame API    | CSV         | {df_csv_time:.2f}            |
| DataFrame API    | Parquet     | {df_parquet_time:.2f}        |
| SQL API          | CSV         | {sql_csv_time:.2f}           |
| SQL API          | Parquet     | {sql_parquet_time:.2f}       |
"""

print(results_table)

spark.stop()

