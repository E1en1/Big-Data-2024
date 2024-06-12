from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

#for geo distance
from pyspark.sql.types import DoubleType
from geopy.distance import geodesic
from pyspark.sql.functions import udf

#Mean
from pyspark.sql.functions import avg, count, expr


#Create a Spark session
spark = SparkSession.builder \
    .appName("ConcatenateDatasets") \
    .getOrCreate()


# Set the logging level to ERROR to suppress INFO and WARN messages
spark.sparkContext.setLogLevel("ERROR")
logging.getLogger("py4j").setLevel(logging.ERROR)





# Load the Crime 2010-2019 CSV file into a DataFrame
crime_2019_df = spark.read.csv('hdfs:///datasets/crime_data_from_2010_to_2019.csv', header=True, inferSchema=True)

# Load the Crime 2019-present CSV file into a DataFrame
crime_present_df = spark.read.csv('hdfs:///datasets/crime_data_from_2020_to_present.csv', header=True, inferSchema=True)

# Concatenate the two DataFrames
crimes_concatenated_df = crime_2019_df.union(crime_present_df)

# Filter the Null Island rows where both x and y are 0
filtered_df =crimes_concatenated_df.filter(~((crimes_concatenated_df['LAT'] == 0) & (crimes_concatenated_df['LON'] == 0)))



#test if concatenation and filtering was successful
# Get the number of rows
num_rows = filtered_df.count()
rows1 =crime_present_df.count()
rows2=crime_2019_df.count()

# Get the number of columns
num_cols = len(filtered_df.columns)
col1 = len(crime_present_df.columns)
col2 = len(crime_2019_df.columns)

# Print the shape of the DataFrame
print("Check the shape of the datasets")

print(f"The shape of the Crime 2010-19 DataFrame is: ({rows2}, {col2})")
print(f"The shape of the Crime 2020-present DataFrame is: ({rows1}, {col1})")
print(f"The shape of the filtered DataFrame is: ({num_rows}, {num_cols})")
print("__________________________________________________________________")




# Select the data where the column "Weapon Used Cd" starts with "1"
crime_df = filtered_df.filter(col("Weapon Used Cd").startswith("1"))


print(f"Next we confirm that we kept only the data for which the column 'Weapon Used Cd' starts with the string 1: {crime_df.show()}")





#Now load the LAPD police station dataset
LAPD_df = spark.read.csv('hdfs:///datasets/LAPD_Police_Stations.csv', header=True, inferSchema=True)


#Inner Join on Crimes and LAPD datasets
join_df = crime_df.join(LAPD_df, crime_df['AREA '] == LAPD_df['PREC'])
join_df.show()





#Compute distance of the crime from the division and add a DISTANCE column to the dataframe
print("Comnputing the distance from each division. . . .")

def geodesic_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

geodesic_distance_udf = udf(geodesic_distance, DoubleType())

crime_with_distance_df = join_df.withColumn("DISTANCE", geodesic_distance_udf("LAT", "LON", "y", "x"))



#Compute the mean distance for each Division
mean_distance_by_division_df = crime_with_distance_df.groupBy("DIVISION").agg(avg("DISTANCE").alias("average_distance"))
mean_distance_by_division_df.show(22)

#Add total incidents column to the new dataframe
incidents_df= crime_with_distance_df.groupBy("DIVISION").agg(count(expr('*')).alias("incidents_total"))

final_df= mean_distance_by_division_df.join(incidents_df,'DIVISION')
final_df.show()

# Stop the Spark session
spark.stop()
