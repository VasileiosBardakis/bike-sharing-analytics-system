import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, col, date_format, hour, avg, corr

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
joined_wj_df = joined_df.join(weather_df, on=['lat', 'lon'],how="inner")

# Weather correlation analysis
# 1. Average utilization rate by temperature ranges
temp_analysis = joined_wj_df.withColumn(
    "temp_range",
    F.when(F.col("temperature") < 10, "Cold (< 10°C)")
     .when(F.col("temperature").between(10, 20), "Mild (10-20°C)")
     .when(F.col("temperature").between(20, 30), "Warm (20-30°C)")
     .otherwise("Hot (> 30°C)")
).groupBy("temp_range").agg(
    avg("utilization_rate").alias("avg_utilization"),
    F.count("*").alias("count")
).orderBy("temp_range")

# 2. Average utilization rate by precipitation ranges
precipitation_analysis = joined_wj_df.withColumn(
    "precip_range",
    F.when(F.col("precipitation") == 0, "No Rain")
     .when(F.col("precipitation").between(0, 2.5), "Light Rain (0-2.5mm)")
     .when(F.col("precipitation").between(2.5, 7.6), "Moderate Rain (2.5-7.6mm)")
     .otherwise("Heavy Rain (>7.6mm)")
).groupBy("precip_range").agg(
    avg("utilization_rate").alias("avg_utilization"),
    F.count("*").alias("count")
).orderBy(F.desc("avg_utilization"))

# 3. Correlation coefficients between weather metrics and utilization
weather_correlations = joined_wj_df.select(
    corr("temperature", "utilization_rate").alias("temperature_correlation"),
    corr("precipitation", "utilization_rate").alias("precipitation_correlation"),
    corr("wind_speed", "utilization_rate").alias("wind_speed_correlation"),
    corr("clouds", "utilization_rate").alias("clouds_correlation")
)

# 4. Hourly analysis with weather conditions
hourly_weather_analysis = joined_wj_df.withColumn(
    "hour", hour(from_unixtime(col("timestamp")))
).groupBy("hour").agg(
    avg("utilization_rate").alias("avg_utilization"),
    avg("temperature").alias("avg_temperature"),
    avg("precipitation").alias("avg_precipitation"),
    avg("wind_speed").alias("avg_wind_speed"),
    avg("clouds").alias("avg_clouds"),
    F.count("*").alias("count")
).orderBy("hour")

# #Show all data
# print("Weather Data:")
weather_df.show()

#stations_info_df.show()

# stations_status_df.show()

#joined_df.show()

#print("Station Utilization rate:")
#joined_df.select("station_id", "name", "utilization_rate").show()

#print("City Utilization rate:")
#total_utilization.show()

joined_wj_df.show()

print("\nUtilization Rate by Temperature Range:")
temp_analysis.show()

print("\nUtilization Rate by Precipitation Range:")
precipitation_analysis.show()

print("\nWeather Metric Correlations with Utilization Rate:")
weather_correlations.show()

print("\nHourly Weather and Utilization Analysis:")
hourly_weather_analysis.show()

time.sleep(600)

# Stop the Spark session
spark.stop()