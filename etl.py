import pandas as pd
import os
import pyspark
from pyspark.sql import SparkSession
import shutil

import csv
import mysql.connector
from dotenv import load_dotenv

# Step 1: Create SparkSession and load the original CSV file
# eventually, I want to extact the data through an API so the data is constanly updated.

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('CrimeDataFiltering').getOrCreate()

# creating a dataframe with the csv file
df = spark.read.format("csv").option("header", "true").load("torontocrime.csv")

# Step 2: Filter the DataFrame with only specific columns
# filtering the dataframe with only specific columns
filtering_df = df.select("OCC_YEAR", "OCC_MONTH", "OCC_DAY", "OCC_HOUR", "OCC_DOW", "PREMISES_TYPE", "MCI_CATEGORY", "NEIGHBOURHOOD_140", "LAT_WGS84", "LONG_WGS84")

# Step 3: Save the filtered DataFrame to a single CSV file

output_folder = "filteredtorontocrime"
filtering_df.coalesce(1).write.csv(output_folder, header=True, mode="overwrite")

# Find the single CSV file in the output folder and rename it
for file in os.listdir(output_folder): # os.listdir returns a list of all the files inside of the directiory
    if file.startswith("part-") and file.endswith(".csv"):
        os.rename(os.path.join(output_folder, file), "filteredtorontocrime.csv") # the renamed file will be placed in the same directory as where the script is ran
        print("Sucessfully renamed file")
        break

# loading the filtered CSV file into a pandas DataFrame
filtered_data = pd.read_csv("filteredtorontocrime.csv")

# fill NaN with default values
filtered_data['OCC_YEAR'] = filtered_data['OCC_YEAR'].fillna(2014)
filtered_data['OCC_MONTH'] = filtered_data['OCC_MONTH'].fillna('January')
filtered_data['OCC_DAY'] = filtered_data['OCC_DAY'].fillna(1)
filtered_data['OCC_DOW'] = filtered_data['OCC_DOW'].fillna('Wednesday')

# checking if there is NaN values 
print(filtered_data.isna().any())

# filtering_df.filter(filtering_df.OBJECTID == "1").show()

# deleting output_folder 
shutil.rmtree(output_folder)

if (os.path.isdir('new_folder') == True):
    print("Output folder was not deleted.")
else:
    print("Output folder sucessfully deleted")

# Step 4: Connect into a database (MySQL) and create a table
load_dotenv()
password_root = os.getenv("password_root")

connection = mysql.connector.connect(
    host="127.0.0.1",          # IP address for localhost
    port=3306,                 # Default MySQL port
    database="torontocrime",   # Database name
    user="root",               # MySQL username 
    password=password_root     
)

if connection.is_connected():
    print("Connection successful") 
else:
    print("Connection unsucessful")

# creating a cursor object using the cursor() method
cursor = connection.cursor()

# dropping EMPLOYEE table if already exists
cursor.execute("DROP TABLE IF EXISTS crime_data")

# creating table 
sql ='''CREATE TABLE crime_data(
   occ_year INT,
   occ_month VARCHAR(100),
   occ_day INT,
   occ_hour INT,
   occ_dow VARCHAR(100),
   premises_type VARCHAR(100),
   mci_category VARCHAR(100),
   neighbourhood VARCHAR(100),
   lat_wgs84 FLOAT,
   long_wgs84 FLOAT
)'''

# execute the CREATE TABLE statement by passing it as a parameter to the execute() method
cursor.execute(sql)

# Step 5: Load CSV file into MySQL database table
# insert the data into MySQL table crime_data
insert_query = '''
INSERT INTO crime_data (
    occ_year, occ_month, occ_day, occ_hour, occ_dow,
    premises_type, mci_category, neighbourhood, lat_wgs84, long_wgs84
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# convert DataFrame to list of tuples (tuples a single row of a table, which contains a single record for that relation)
data_tuples = [tuple(x) for x in filtered_data.to_numpy()]

# use executemany to insert all rows efficiently inside of table
cursor.executemany(insert_query, data_tuples)
connection.commit()

print(f"{cursor.rowcount} rows inserted successfully.")

# close connection
cursor.close()
connection.close()


