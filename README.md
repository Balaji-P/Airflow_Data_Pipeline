# Airflow-data-Pipline
 This project demonstrates an Apache Airflow data pipeline for loading employee data from an S3 bucket to Snowflake. The pipeline automates the process of extracting data from PostgreSQL, uploading it to S3, joining and detecting new or changed rows, and finally loading the data into Snowflake.


# Project Overview
The main objective of this project is to enable seamless synchronization of employee salary data between the PostgreSQL source system and the Snowflake data warehouse. The project follows these key steps:

Extraction: Utilizing the PostgresOperator, the project extracts employee salary data from the PostgreSQL source system hosted on AWS. This step requires appropriate credentials and permissions to access the source system.
Staging: The extracted data is then staged in the Amazon S3 storage system using the S3UploadOperator. This operator securely uploads the data to a designated S3 bucket, preparing it for further processing.
Transformation: The project applies custom transformations to the extracted data using the powerful data manipulation capabilities of Pandas. The transformations are executed within the PythonOperator, allowing flexibility and customization in the transformation logic.
Update and Insert: Leveraging the SnowflakeOperator, the project updates existing employee salary records in the Snowflake data warehouse. The BranchPythonOperator is utilized to check if an employee record already exists in the data warehouse. If the record exists, it performs an update; otherwise, it inserts the new record.

# Project Structure
The project structure is organized as follows:

dags/: Directory containing the Airflow DAG definition file.

includes/: Directory containing custom Python modules used in the DAG.

queries/: Directory containing SQL queries used in the DAG.


![image](https://github.com/HabibaaMohey/Airflow-data-Pipline/assets/132647130/1c583e51-7a19-40f7-a13b-4c22ac738b1a)

