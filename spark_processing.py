import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, col, date_format, hour, avg, corr

#Define the relative output directory
output_dir = "output"

#Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

#Initialize Spark session
spark = SparkSession.builder \
    .appName("BikeSharingAnalysis") \
    .master("local[*]") \
    .getOrCreate()

#Set logging level to WARN or ERROR to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

#Load Parquet files into DataFrames
weather_df = spark.read.parquet("data_parquet/weather_data.parquet")
stations_info_df = spark.read.parquet("data_parquet/stations_info.parquet")
stations_status_df = spark.read.parquet("data_parquet/stations_status.parquet")


#SPARK SQL QUERIES

############# Join station information with status data ################

joined_df = stations_info_df.join(stations_status_df, on="station_id", how="inner")

############# Calculate utilization rates per station and across the city ##########

#station
joined_df = joined_df.withColumn(
    "utilization_rate",
    (F.col("capacity") - F.col("num_docks_available")) / F.col("capacity")
)

#city
total_utilization = joined_df.agg(
    F.sum(F.col("capacity") - F.col("num_docks_available")).alias("total_bikes_used"),
    F.sum("capacity").alias("total_capacity")
).withColumn(
    "city_utilization_rate",
    F.col("total_bikes_used") / F.col("total_capacity")
)

########### Weather correlation analysis ##################

#JOIN joined_df with weather
joined_wj_df = joined_df.join(weather_df, on=['lat', 'lon'],how="inner")

#1.Average utilization rate by temperature ranges
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

#2.Average utilization rate by precipitation ranges
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

#3.Correlation coefficients between weather metrics and utilization
weather_correlations = joined_wj_df.select(
    corr("temperature", "utilization_rate").alias("temperature_correlation"),
    corr("precipitation", "utilization_rate").alias("precipitation_correlation"),
    corr("wind_speed", "utilization_rate").alias("wind_speed_correlation"),
    corr("clouds", "utilization_rate").alias("clouds_correlation")
)

############## HOURLY ANALYSIS WITH WEATHER CONDITIONS ########################

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

############## Overall system utilization on a single dataframe, including weather conditions #############

#Extract the correlation values from the weather_correlations DataFrame
correlation_values = weather_correlations.collect()[0]

#Add the correlation values as new columns to the joined_wj_df
final_df = joined_wj_df.withColumn(
    "temperature_correlation", F.lit(correlation_values["temperature_correlation"])
).withColumn(
    "precipitation_correlation", F.lit(correlation_values["precipitation_correlation"])
).withColumn(
    "wind_speed_correlation", F.lit(correlation_values["wind_speed_correlation"])
).withColumn(
    "clouds_correlation", F.lit(correlation_values["clouds_correlation"])
)


############# Save the usage summaries (dataframe) to a database or other storage ###############

# Save the DataFrame to a CSV file in the output directory
# hourly_weather_analysis.write.mode("overwrite").csv(os.path.join(output_dir, "hourly_weather_analysis.csv"))

# Show all data

print("Weather Data:")
weather_df.show()

print("Station information:")
stations_info_df.show()

print("Station status:")
stations_status_df.show()

print("Join station information with status data:")
joined_df.show()

print("Station Utilization rate:")
joined_df.select("station_id", "name", "utilization_rate").show()

print("City Utilization rate:")
total_utilization.show()

#joined_wj_df.show()

print("\nCorrelations of weather conditions and bike usage:")
print("\nUtilization Rate by Temperature Range:")
temp_analysis.show()

print("\nUtilization Rate by Precipitation Range:")
precipitation_analysis.show()

print("\nWeather Metric Correlations with Utilization Rate:")
weather_correlations.show()

print("\nHourly Weather and Utilization Analysis:")
hourly_weather_analysis.show()

print("\nFinal Table with Weather Conditions and Correlations:")
final_df.show()

# Stop the Spark session
spark.stop()