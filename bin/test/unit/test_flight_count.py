import unittest
import os
from pyspark.sql import SparkSession

class TestFlightCount(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.5.jar") \
            .appName("test_flightcount") \
            .master("local[*]") \
            .getOrCreate()

    def test_flightcount(self):
        # Ensure that the CSV file exists
        self.assertTrue(os.path.exists("flightsdest.csv"))

        # Read the CSV file into a PySpark DataFrame
        sparkDF_dest = self.spark.read.csv("flightsdest.csv", header=True)

        # Group the DataFrame by the 'country' column and count the number of occurrences
        df_country = sparkDF_dest.groupBy("country").count()

        # Ensure that there are at least 1 row in the DataFrame
        self.assertGreaterEqual(df_country.count(), 1)

    def tearDown(self):
        # Delete the CSV file
        os.remove("flightsdest.csv")

if __name__ == '__main__':
    unittest.main()
