#!/usr/bin/python


#
# Author: Susmita Vikas Mhamane
# Date: July 11, 2024

# Import necessary libraries from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc

# Create a SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Show existing tables in the 'electric' database
print(spark.catalog.listTables())

# Get the current Spark SQL warehouse directory
print(spark.conf.get('spark.sql.warehouse.dir'))

# Print a message indicating DataFrame creation
print("Creating DataFrame--------------------------------------------------------")
# Create a DataFrame from the file path
file_path = "Electric_Vehicle_Population_Data_LabExam.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)


# Print the schema of the DataFrame
df.printSchema()

# Convert all column names to lower case for consistency
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# Drop rows with any null values
clean_df = df.na.drop()
# Filter the cleaned DataFrame for Battery Electric Vehicles (BEV)
bev_df = clean_df.filter(clean_df['electric_vehicle_type'] == "Battery Electric Vehicle (BEV)")

# Group by City and count the number of BEVs in each city
city_bev_counts = bev_df.groupBy("city").count()

# Find the city with the highest count of BEVs
city_with_most_bev = city_bev_counts.orderBy(city_bev_counts["count"].desc()).first()

# Print the result
if city_with_most_bev:
    print("City with the most number of Battery Electric Vehicles (BEV):", city_with_most_bev["city"])
    print("Number of BEVs in this city:", city_with_most_bev["count"])
else:
    print("No city found with Battery Electric Vehicles (BEV)")

# Find the car with the highest "electric_range"
car_with_highest_range = clean_df.filter(clean_df['electric_range'].isNotNull()) \
                                .orderBy(desc("electric_range")) \
                                .first()

# Print the result
if car_with_highest_range:
    # Check if "make" and "electric_range" columns exist in the DataFrame
    if "make" in car_with_highest_range and "model" in car_with_highest_range and "electric_range" in car_with_highest_range:
        print("Car with the highest Electric Range:")
        print("Make:", car_with_highest_range["make"])
        print("Model:", car_with_highest_range["model"])
        print("Electric Range:", car_with_highest_range["electric_range"])
    else:
        print("Make or Electric_Range not found in the result row.")
else:
    print("No car found with Electric Range information")

# Verify if 'postal_code' exists in the DataFrame
if 'postal_code' in clean_df.columns:
    # Find the "make" and "model" of the car most bought in area with Postal Code "98126"
    most_bought_car = clean_df.filter(clean_df.postal_code == "98126") \
                             .groupBy("make", "model") \
                             .count() \
                             .orderBy(desc("count")) \
                             .first()

    # Print the result
    if most_bought_car:
        print(f"Most bought car in Postal Code 98126:")
        print(f"Make: {most_bought_car['make']}")
        print(f"Model: {most_bought_car['model']}")
        print(f"Number of purchases: {most_bought_car['count']}")
    else:
        print("No data found for Postal Code 98126")
else:
    print("Column 'postal_code' not found in DataFrame")

# Filter the DataFrame for cars with CAFV_Eligibility as "Not eligible due to low battery range"
not_eligible_cars = clean_df.filter(clean_df['cafv_eligibility'] == "Not eligible due to low battery range")

# Group by model_year and count the number of cars for each year
yearly_counts = not_eligible_cars.groupBy("model_year").count()

# Find the year with the maximum count
max_cars_year = yearly_counts.orderBy(desc("count")).first()

# Print the result
if max_cars_year:
    print(f"Year with maximum cars not eligible due to low battery range:")
    print(f"model_year: {max_cars_year['model_year']}")
    print(f"Number of cars: {max_cars_year['count']}")
else:
    print("No data found for cars not eligible due to low battery range")

# Filter the DataFrame for Tesla Model S cars
tesla_model_s_df = clean_df.filter((clean_df['make'] == "Tesla") & (clean_df['model'] == "Model S"))

# Find the Tesla Model S with the highest Electric_Range
max_range_tesla_model_s = tesla_model_s_df.orderBy(desc("electric_range")).first()

# Print the result
if max_range_tesla_model_s:
    print("Tesla Model S with the max Electric Range:")
    print("Make:", max_range_tesla_model_s["make"])
    print("Model:", max_range_tesla_model_s["model"])
    print("Electric Range:", max_range_tesla_model_s["electric_range"])
else:
    print("No Tesla Model S found with Electric Range information")

# Define the DataFrame to save into Hive table
evms_us_df = clean_df.select("county", "city", "state", "make", "model", "electric_vehicle_type")

# Save DataFrame to Hive table 'electricVehicle' with partitioning on State, City, and county
evms_us_df.write \
    .partitionBy("state", "city","county") \
    .mode("overwrite") \
    .saveAsTable("electric.electricVehicle")

print("DataFrame stored into Hive table 'electricVehicle' successfully")

'''
Choosing the Storage Format
For optimal performance, especially in terms of query execution and storage efficiency, Parquet format is recommended when storing data into Hive tables. Parquet is a columnar storage format that provides benefits such as:

Efficient compression and encoding, reducing storage space.
Columnar storage which enables faster query processing by minimizing I/O operations.
Support for predicate pushdown, which can further optimize queries by reading only the necessary data.
'''
