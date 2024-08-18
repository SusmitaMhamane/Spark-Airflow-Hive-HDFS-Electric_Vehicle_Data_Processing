## Author: Susmita
## Date: 11 July,2024


# Title: Electric Vehicle Data Processing with Airflow

## Project Overview:

This project demonstrates data processing workflows using Apache Airflow for handling electric vehicle population data. The Airflow DAG (`Electric_vehicle`) orchestrates tasks including file upload to HDFS, data transformation with Spark, and data querying using Hive.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## Prerequisites:

- Apache Airflow installed and configured.
- Python 3.6 or higher.
- Hadoop HDFS configured.
- Spark environment configured (`spark_default` connection).
- Hive installed and configured.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## DAG Configuration:

The `Electric_vehicle` DAG is configured to run manually (`schedule_interval=None`) and consists of three tasks:

- `upload_file`: Executes `Electric_Vehicle.sh` to upload a CSV file to HDFS.
- `data_transform`: Submits `electricVehicle.py` to Spark for data transformation.
- `data_query`: Runs a Hive query to retrieve data from the `electric.electricVehicle` table.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## Scripts Overview:

### Electric_Vehicle.sh

- **Purpose:** Uploads `Electric_Vehicle_Population_Data_LabExam.csv` to HDFS.
- **Usage:** Ensure executable (`chmod +x Electric_Vehicle.sh`).
- **Dependencies:** Requires Hadoop HDFS.

### electricVehicle.py

- **Purpose:** Performs data transformation and analysis using PySpark.
- **Dependencies:** PySpark, Hive setup.
- **Output:** Stores transformed data into `electric.electricVehicle` Hive table.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## Usage:

1. **DAG Execution:**
   - Place `Electric_vehicle.py` in Airflow's DAGs folder.
   - Trigger the DAG manually via Airflow UI or CLI (`airflow trigger_dag Electric_vehicle`).

2. **Script Execution:**
   - Ensure `Electric_Vehicle.sh` is executable (`chmod +x Electric_Vehicle.sh`).
   - Run the script manually or via Airflow's `PythonOperator`.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## DAG Configuration:

- **DAG ID**: Electric_vehicle
- **Start Date**: 1 day ago from the current date
- **Schedule Interval**: None (Manual execution or triggered externally)
- **Requirements**:
  - Apache Airflow (Version X.X.X)
  - Apache Spark (Version 2.5.1)
  - Hadoop Distributed File System (HDFS)
  - Hive

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### Pre-requisites:

- Ensure Apache Airflow, Spark, HDFS, and Hive are correctly installed and configured.

### Setup:

- Place the `electricVehicle.py` script in the appropriate directory accessible by Airflow.
- Ensure the CSV file (`Electric_Vehicle_Population_Data_LabExam.csv`) is available and accessible for uploading to HDFS.

### Execution:

- Trigger the DAG manually via the Airflow UI or programmatically.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Output:

- Check Airflow logs and UI for task execution details.
- Verify the Hive table `electric.electricVehicle` for transformed data.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Choosing the Storage Format

### For optimal performance, especially in terms of query execution and storage efficiency, Parquet format is recommended when storing data into Hive tables. Parquet is a columnar storage format that provides benefits such as:

- **Efficient compression and encoding**, reducing storage space.
- **Columnar storage** which enables **faster query processing** by minimizing I/O operations.
- **Support for predicate pushdown**, which can further optimize queries by reading only the necessary data.

By leveraging the benefits of the Parquet storage format, you can optimize both storage utilization and query performance in your data pipelines, making it an ideal choice for storing analytical data in Hive tables.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Conclusion:

This DAG automates the end-to-end process of handling Electric Vehicle population data, from ensuring data freshness by checking and deleting existing files in HDFS, to transforming data using Spark, and finally querying insights from a Hive table. By leveraging Airflow's task orchestration capabilities, this pipeline enhances efficiency and reliability in data processing and analysis workflows.



