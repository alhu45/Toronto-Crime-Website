import pyspark
import pandas as pd
import os

from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('CrimeDataFiltering').getOrCreate()

# creating a dataframe with the csv file
df = spark.read.format("csv").option("header", "true").load("torontocrime.csv")

# filtering the dataframe with only specific columns
filtering_df = df.select("OCC_YEAR", "OCC_MONTH", "OCC_DAY", "OCC_HOUR", "OCC_DOW", "PREMISES_TYPE", "MCI_CATEGORY", "NEIGHBOURHOOD_140", "LAT_WGS84", "LONG_WGS84")


filtering_df.coalesce(1).write.csv("filteredtorontocrime", header=True)



# filtering_df.filter(filtering_df.OBJECTID == "1").show()





