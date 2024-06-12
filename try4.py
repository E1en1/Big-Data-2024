from pyspark import SparkConf, SparkContext
from pyspark.broadcast import Broadcast
from geopy.distance import geodesic
import csv


def parse_row(row):
    csv.field_size_limit(1_600_000)
    reader = csv.reader([row], dialect="excel")
    fields = next(reader)
    return fields


# Initialize SparkContext
conf = SparkConf().setAppName("CrimeDataAnalysis")
sc = SparkContext(conf=conf)

# Function to parse the crime data
def parse_crime(line):
    csv.field_size_limit(1_600_000)
    reader = csv.reader([line], dialect="excel")
    fields = next(reader)

    return (fields[4], fields[14], float(fields[26]), float(fields[27]))  # (AREA, WEAPON_USED, LAT, LON)

# Function to parse the police department data
def parse_police(line):
    csv.field_size_limit(1_600_000)
    reader = csv.reader([line], dialect="excel")
    fields = next(reader)

    return (fields[5], (fields[3], float(fields[1]), float(fields[0])))  # (PREC, DIVISION, y, x)





# Load the Crime 2010-2019 CSV file into an RDD
crime_2019_rdd = sc.textFile('hdfs:///datasets/crime_data_from_2010_to_2019.csv')

# Extract the header row
header = crime_2019_rdd.first()

# Filter out the header row
crime_2019_rdd = crime_2019_rdd.filter(lambda line: line != header) 
print(crime_2019_rdd.take(5))


# Load the Crime 2019-present CSV file into an RDD
crime_present_rdd = sc.textFile('hdfs:///datasets/crime_data_from_2020_to_present.csv')

# Extract the header row
header = crime_present_rdd.first()

# Filter out the header row
crime_present_rdd = crime_present_rdd.filter(lambda line: line != header) 








# Concatenate the two RDDs
crime_data = crime_2019_rdd.union(crime_present_rdd).map(parse_crime)


# Filter the crime data
filtered_crime_data = crime_data.filter(lambda x: x[2] != 0 and x[3] != 0)  # Filter out rows with LAT = 0 and LON = 0
filtered_crime_data = filtered_crime_data.filter(lambda x: x[1].startswith("1"))  # Filter rows with WEAPON_USED starts with "1"

# Read the police department data file
police_data = sc.textFile("hdfs:///datasets/LAPD_Police_Stations.csv")

# Extract the header row
header = police_data.first()

# Filter out the header row
police_data = police_data.filter(lambda line: line != header).map(parse_police)






# Collect the police department data to broadcast
police_data_broadcast = sc.broadcast(police_data.collectAsMap())

# Function to calculate distance and map to police station
def map_to_police_station(crime):
    area, weapon_used, lat, lon = crime
    min_distance = float('inf')
    nearest_police_station = None
    for prec, (division, y, x) in police_data_broadcast.value.items():
        if prec == area:
            distance = geodesic((lat, lon), (y, x)).miles
            if distance < min_distance:
                min_distance = distance
                nearest_police_station = division
    return (nearest_police_station, (min_distance, 1))

# Map the crime data to police stations with distances
mapped_data = filtered_crime_data.map(map_to_police_station)

# Reduce by key to calculate total distance and number of incidents per police station
reduced_data = mapped_data.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Calculate the mean distance and total number of incidents per police station
result = reduced_data.mapValues(lambda x: (x[0] / x[1], x[1]))

# Collect and print the results
output = result.collect()
for police_station, (mean_distance, total_incidents) in output:
    print(f"Police Station: {police_station}, Mean Distance: {mean_distance}, Total Incidents: {total_incidents}")

# Stop SparkContext
sc.stop()
