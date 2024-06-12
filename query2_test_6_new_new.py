from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark import SparkConf
import time

# Initialize Spark
conf = SparkConf().setAppName("CrimeAnalysisQ2").set("spark.executor.memory", "2g").set("spark.executor.cores", "2")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Read the CSV files
csv_file_path1 = 'hdfs:///datasets/crime_data_from_2010_to_2019.csv'
csv_file_path2 = 'hdfs:///datasets/crime_data_from_2020_to_present.csv'

# Load data as DataFrames
start_time_df = time.time()
df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True, quote='"', escape='"')
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True, quote='"', escape='"')
end_time_df = time.time()

# Combine the DataFrames
crime_df = df1.union(df2)

# Define time ranges for different parts of the day
morning = (5 * 60, 11 * 60 + 59)  # 5:00 AM - 11:59 AM
afternoon = (12 * 60, 16 * 60 + 59)  # 12:00 PM - 4:59 PM
evening = (17 * 60, 20 * 60 + 59)  # 5:00 PM - 8:59 PM
night = (21 * 60, 4 * 60 + 59)  # 9:00 PM - 4:59 AM (next day)

# Add a new column 'PartOfDay' based on crime occurrence time
crime_df = crime_df.withColumn('PartOfDay',
    when((col('TIME OCC') >= morning[0]) & (col('TIME OCC') <= morning[1]), 'Morning')
    .when((col('TIME OCC') >= afternoon[0]) & (col('TIME OCC') <= afternoon[1]), 'Afternoon')
    .when((col('TIME OCC') >= evening[0]) & (col('TIME OCC') <= evening[1]), 'Evening')
    .otherwise('Night')
)

# Filter records for street crimes
street_crimes_df = crime_df.filter(col('Premis Desc') == 'STREET')

# Group by part of the day and count crimes
start_time_df_query = time.time()
part_of_day_counts_df = street_crimes_df.groupBy('PartOfDay').count()
end_time_df_query = time.time()

# Sort by crime count in descending order
sorted_part_of_day_counts_df = part_of_day_counts_df.orderBy(col('count').desc())

# Print DataFrame API results
print("DataFrame API Results:")
sorted_part_of_day_counts_df.show(truncate=False, n=sorted_part_of_day_counts_df.count())

# Load data as RDDs
start_time_rdd = time.time()
rdd1 = sc.textFile(csv_file_path1)
rdd2 = sc.textFile(csv_file_path2)
end_time_rdd = time.time()

# Combine the RDDs
combined_rdd = rdd1.union(rdd2)

# Function to parse CSV lines correctly handling quotes and commas
def parse_csv_line(line):
    import csv
    from io import StringIO
    reader = csv.reader(StringIO(line))
    return next(reader)

# Parse the RDD
parsed_rdd = combined_rdd.map(parse_csv_line)

# Filter records for street crimes using RDD and create (PartOfDay, 1) pairs
part_of_day_counts_rdd = parsed_rdd.filter(lambda fields: fields[15] == 'STREET') \
    .map(lambda fields: (
        'Morning' if morning[0] <= int(fields[3]) <= morning[1] else
        'Afternoon' if afternoon[0] <= int(fields[3]) <= afternoon[1] else
        'Evening' if evening[0] <= int(fields[3]) <= evening[1] else
        'Night'
    , 1))

# Reduce by key to get counts for each part of the day
part_of_day_counts_rdd = part_of_day_counts_rdd.reduceByKey(lambda a, b: a + b)

# Print RDD API results
print("\nRDD API Results:")
for part, count in part_of_day_counts_rdd.collect():
    print(f"{part}: {count}")

# Calculate execution times
df_load_time = end_time_df - start_time_df
df_query_time = end_time_df_query - start_time_df_query
rdd_load_time = end_time_rdd - start_time_rdd
rdd_query_time = time.time() - end_time_rdd

# Create a comparison table
print("\nExecution Times:")
print(f"DataFrame Load Time: {df_load_time:.4f} seconds")
print(f"DataFrame Query Time: {df_query_time:.4f} seconds")
print(f"RDD Load Time: {rdd_load_time:.4f} seconds")
print(f"RDD Query Time: {rdd_query_time:.4f} seconds")

# Stop Spark
spark.stop()

