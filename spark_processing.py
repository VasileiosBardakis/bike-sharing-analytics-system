import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import hour

spark = SparkSession.builder \
    .appName("BikeSharingAnalysis") \
    .master("local[*]") \
    .getOrCreate()

#Load Parquet files into DataFrames
weather_df = spark.read.parquet("data_parquet/weather_data.parquet")
stations_info_df = spark.read.parquet("data_parquet/stations_info.parquet")
stations_status_df = spark.read.parquet("data_parquet/stations_status.parquet")


#SPARK SQL QUERIES

#Join DataFrames on station_id
joined_df = stations_info_df.join(stations_status_df, on="station_id", how="inner")

#Calculate utilization rate per station
utilization_df = joined_df.withColumn(
    "utilization_rate",
    (F.col("capacity") - F.col("num_docks_available")) / F.col("capacity")
)

#Calculate total utilization rate across the city
total_utilization = utilization_df.agg(
    F.sum(F.col("capacity") - F.col("num_docks_available")).alias("total_bikes_used"),
    F.sum("capacity").alias("total_capacity")
).withColumn(
    "city_utilization_rate",
    F.col("total_bikes_used") / F.col("total_capacity")
)

#Show all data
print("Weather Data:")
weather_df.show()

print("Station Information:")
stations_info_df.show()

print("Station Status:")
stations_status_df.show()

print("Joined DFs:")
joined_df.show()

print("Station Utilization rate:")
utilization_df.select("station_id", "name", "utilization_rate").show()

print("City Utilization rate:")
total_utilization.show()


time.sleep(600)

# Stop the Spark session
spark.stop()