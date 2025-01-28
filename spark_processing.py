import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, col, date_format, hour

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
joined_df = joined_df.withColumn(
    "utilization_rate",
    (F.col("capacity") - F.col("num_docks_available")) / F.col("capacity")
)

#Calculate total utilization rate across the city
total_utilization = joined_df.agg(
    F.sum(F.col("capacity") - F.col("num_docks_available")).alias("total_bikes_used"),
    F.sum("capacity").alias("total_capacity")
).withColumn(
    "city_utilization_rate",
    F.col("total_bikes_used") / F.col("total_capacity")
)

# Convert timestamp as 'yyyy-MM-dd HH'
weather_df = weather_df.withColumn(
    "time", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd HH")
)

#Convert last_reported timestamp
joined_df = joined_df.withColumn(
    "time", date_format(from_unixtime(col("last_reported")), "yyyy-MM-dd HH")
)

#Join DataFrames on col time lat lon
joined_wj_df = joined_df.join(weather_df, on=['time', 'lat', 'lon'],how="inner")

# #Show all data
# print("Weather Data:")
weather_df.show()

stations_info_df.show()

# stations_status_df.show()

joined_df.show()

#print("Station Utilization rate:")
#joined_df.select("station_id", "name", "utilization_rate").show()

#print("City Utilization rate:")
#total_utilization.show()

joined_wj_df.show()

time.sleep(600)

# Stop the Spark session
spark.stop()