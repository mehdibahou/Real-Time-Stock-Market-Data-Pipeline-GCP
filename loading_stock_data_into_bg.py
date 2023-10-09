from google.cloud import bigquery
import os
import csv
import time
# Set the path to your JSON key file
keyfile_path = "orbital-surge-359121-1dd5e9f4ccac.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_path

# Set your Google Cloud project ID
project_id = "orbital-surge-359121"  # Replace with your actual project ID

# Initialize a BigQuery client
client = bigquery.Client(project=project_id)

# Set the GCS URI for the data to be loaded
gcs_uri = "gs://ayoubstockdata/output_with_timestamp-00000-of-00001"

# Set the BigQuery dataset and table information
dataset_id = "stockdataset"  # Remove the .json extension
table_id = "Stock_table2"  # Remove the .json extension

# Define the schema of the table (optional if schema is not inferred)
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "INTEGER")
]

# Configure the job for data loading
job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,  # If your data has a header row
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # or WRITE_TRUNCATE, etc.
)

# Start the BigQuery job to load data from GCS
job = client.load_table_from_uri(
    gcs_uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
)

# Wait for the job to complete
job.result()

print(f"Data loaded from {gcs_uri} to {project_id}.{dataset_id}.{table_id}")

# SQL code to execute after data loading
sql_code = """
ALTER TABLE `orbital-surge-359121.stockdataset.Stock_table2`
ADD COLUMN timestamp_as_string STRING;

UPDATE `orbital-surge-359121.stockdataset.Stock_table2`
SET timestamp_as_string = CAST(timestamp AS STRING)
WHERE TRUE;

UPDATE `orbital-surge-359121.stockdataset.Stock_table2`
SET timestamp_as_string = REGEXP_REPLACE(timestamp_as_string, r'\\+00', '')
WHERE TRUE;

"""

sql_code1 = """
ALTER TABLE `orbital-surge-359121.stockdataset.Stock_table2`
ADD COLUMN date_column DATE,
ADD COLUMN time_column TIME;

UPDATE `orbital-surge-359121.stockdataset.Stock_table2`
SET
  date_column = CAST(SPLIT(timestamp_as_string, ' ')[SAFE_OFFSET(0)] AS DATE),
  time_column = CAST(SPLIT(timestamp_as_string, ' ')[SAFE_OFFSET(1)] AS TIME)
WHERE TRUE;

"""
sql_code2= """
ALTER TABLE `orbital-surge-359121.stockdataset.Stock_table2`
DROP COLUMN timestamp ;

ALTER TABLE `stockdataset.Stock_table2`
RENAME COLUMN timestamp_as_string TO Timestamp ;

"""

# Run the SQL query
query_job = client.query(sql_code)
query_job = client.query(sql_code1)
query_job = client.query(sql_code2)



time.sleep(15)

# Wait for the query to complete
query_job.result()

# Print the results if needed
for row in query_job:
    # Process the query results here
    print(row)