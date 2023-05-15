import requests
import pyspark
import pandas as pd
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

url = "https://api.schiphol.nl/public-flights/flights"

headers = {
    'app_id': "55755366",
    'app_key': "9c9e8d9361be386109b36997ef2815c6",
    'accept': "application/json",
    'resourceversion': "v4",
    'cache-control': "no-cache",
    'postman-token': "5a4f8ea8-bd69-faa5-9788-8f358abc4011"
}

# Get data from API and save as CSV
r = requests.request("GET", url, headers=headers)
x = r.json()
df_flights = pd.json_normalize(x, 'flights')
df_flights.to_csv('flightsdata.csv', mode='a', index=False, header=True)

# Read CSV data and filter using Spark
df1_flights = pd.read_csv("./flightsdata.csv", sep=",", on_bad_lines='skip') \
                .drop_duplicates() \
                .to_csv('flightsdatafinal.csv', index=False)

spark = SparkSession.builder \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.5.jar") \
    .appName("flightdelay") \
    .getOrCreate()

sparkDF_flights = spark.read.csv("./flightsdatafinal.csv", header=True)

arrival_df = sparkDF_flights.select("lastUpdatedAt", 
                                     "actualLandingTime", 
                                     "estimatedLandingTime", 
                                     "flightName", 
                                     "`publicFlightState.flightStates`")

filtered_data = arrival_df.filter(col("`publicFlightState.flightStates`").isin("['ARR']", "['ARR', 'EXP']"))

# Calculate time differences and filter delayed flights
df2_diff = filtered_data.withColumn('actualLandingTime',to_timestamp(col('actualLandingTime'))) \
                        .withColumn('estimatedLandingTime', to_timestamp(col('estimatedLandingTime'))) \
                        .withColumn('DiffInSeconds', col("actualLandingTime").cast("long") - col('estimatedLandingTime').cast("long"))

threshold = 10
late_flights = df2_diff.filter(col("DiffInSeconds") > threshold)

# Write delayed flights to a PostgreSQL table
late_flights.write.format("jdbc") \
                .option("url", "jdbc:postgresql://192.168.68.72:5432/airflow") \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "delayedflight") \
                .option("user", "airflow") \
                .option("password", "airflow") \
                .save()

