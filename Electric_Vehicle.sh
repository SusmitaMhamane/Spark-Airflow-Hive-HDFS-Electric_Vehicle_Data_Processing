#!/bin/bash


# Author: Susmita
# Date: 12 July 2024

source_file="/home/talentum/shared/Electric_Vehicle_Population_Data_LabExam.csv"
hdfs_target_dir="hdfs://localhost:9000/user/talentum/"
hdfs_file_name="Electric_Vehicle_Population_Data_LabExam.csv"

# Check if file exists in HDFS and delete if present
if hdfs dfs -test -e "${hdfs_target_dir}${hdfs_file_name}"; then
    echo "File already exists in HDFS. Deleting existing file..."
    if hdfs dfs -rm "${hdfs_target_dir}${hdfs_file_name}"; then
        echo "File deleted successfully."
    else
        echo "Failed to delete existing file. Exiting."
        exit 1
    fi
fi

# Copy file to HDFS
echo "Copying file to HDFS..."
if hdfs dfs -put "${source_file}" "${hdfs_target_dir}"; then
    echo "File copied successfully to HDFS."
else
    echo "Failed to copy file to HDFS. Exiting."
    exit 1
fi

# Verify file existence in HDFS after upload
echo "Verifying file existence in HDFS..."
hdfs dfs -ls "${hdfs_target_dir}${hdfs_file_name}"

