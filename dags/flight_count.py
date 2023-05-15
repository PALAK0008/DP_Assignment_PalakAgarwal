import requests
import pyspark
import pandas as pd
import json
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Define the URL and headers for the API request
url = "https://api.schiphol.nl/public-flights/destinations"
headers = {
    'app_id': "55755366",
    'app_key': "9c9e8d9361be386109b36997ef2815c6",
    'accept': "application/json",
    'resourceversion': "v4",
    'cache-control': "no-cache",
    'postman-token': "5a4f8ea8-bd69-faa5-9788-8f358abc4011"
}

# Make the API request and convert the response to a JSON object
response = requests.get(url, headers=headers)
json_data = response.json()

# Convert the JSON object to a Pandas DataFrame and save it to a CSV file
df_dest = pd.json_normalize(json_data, 'destinations')
df_dest.to_csv('flightsdest.csv', index=False, header=True)

# Create a PySpark SparkSession
spark = SparkSession.builder \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.5.jar") \
    .appName("flightcount") \
    .getOrCreate()

# Read the CSV file into a PySpark DataFrame
sparkDF_dest = spark.read.csv("flightsdest.csv", header=True)

# Group the DataFrame by the 'country' column and count the number of occurrences
df_country = sparkDF_dest.groupBy("country").count()
df_country.show(50, truncate=False)

# Write the grouped DataFrame to a PostgreSQL table
df_country.write.format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.68.72:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "flightsperday") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .save()

