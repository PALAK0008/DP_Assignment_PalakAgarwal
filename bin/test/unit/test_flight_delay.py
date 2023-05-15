import unittest
import os.path
from datetime import datetime
from pyspark.sql import SparkSession

class TestFlightsData(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("TestFlightsData").master("local[2]").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_delayed_flights(self):
        from flights_data import process_flights_data

        # Call the function to be tested
        process_flights_data()

        # Verify that the output file was created and contains data
        self.assertTrue(os.path.isfile('./delayed_flights.csv'))
        df = self.spark.read.format('csv').option('header', True).load('./delayed_flights.csv')
        self.assertGreater(df.count(), 0)

        # Verify that the output file contains the expected columns
        expected_cols = ['lastUpdatedAt', 'actualLandingTime', 'estimatedLandingTime', 'flightName', 'publicFlightState.flightStates', 'DiffInSeconds']
        self.assertListEqual(expected_cols, df.columns)

        # Verify that the output file contains only delayed flights
        threshold = 10
        delayed_df = df.filter(df.DiffInSeconds > threshold)
        self.assertEqual(delayed_df.count(), df.count())

        # Verify that the timestamps were correctly parsed
        for col in ['actualLandingTime', 'estimatedLandingTime']:
            self.assertIsInstance(df.select(col).first()[col], datetime)

    def tearDown(self):
        self.spark.stop()
        os.remove('./flightsdata.csv')
        os.remove('./flightsdatafinal.csv')
        os.remove('./delayed_flights.csv')

if __name__ == '__main__':
    unittest.main()
