import os
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
from datetime import timedelta
from datetime import datetime

# Set up your Google Cloud credentials and BigQuery parameters
PROJECT_ID = 'data-engineer-zoom-camp'
DATASET_ID = 'staging_dataset'
CURATED_DATASET_ID = 'curated_dataset'
TABLE_ID = 'md_covid'
GCS_BUCKET_NAME = 'data_engineer_zoom_camp'  # GCS Bucket to temporarily store CSV data
GCS_PATH = 'md_covid/data.csv'  # Path in GCS where CSV will be stored
truncate_and_insert_sql = f"""
TRUNCATE TABLE `{PROJECT_ID}.{CURATED_DATASET_ID}.md_covid`;

INSERT INTO `{PROJECT_ID}.{CURATED_DATASET_ID}.md_covid` (
    OBJECTID, DATE, Allegany, Anne_Arundel, Baltimore, Baltimore_City, Calvert,
    Caroline, Carroll, Cecil, Charles, Dorchester, Frederick, Garrett, Harford,
    Howard, Kent, Montgomery, Prince_Georges, Queen_Annes, Somerset, St_Marys,
    Talbot, Washington, Wicomico, Worcester, Unknown
)
SELECT
    OBJECTID,
    DATE(PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', DATE)) AS DATE,
    Allegany, Anne_Arundel, Baltimore, Baltimore_City, Calvert, Caroline, Carroll,
    Cecil, Charles, Dorchester, Frederick, Garrett, Harford, Howard, Kent,
    Montgomery, Prince_Georges, Queen_Annes, Somerset, St_Marys, Talbot, Washington,
    Wicomico, Worcester, Unknown
FROM `{PROJECT_ID}.{DATASET_ID}.md_covid`;

truncate table `curated_dataset.md_covid_breakdown`;
insert into `curated_dataset.md_covid_breakdown`
select DATE,'Allegany' as County,Allegany as amount from curated_dataset.md_covid union all
select DATE,'Anne_Arundel' as County,Anne_Arundel as amount from curated_dataset.md_covid union all
select DATE,'Baltimore' as County,Baltimore as amount from curated_dataset.md_covid union all
select DATE,'Baltimore_City' as County,Baltimore_City as amount from curated_dataset.md_covid union all
select DATE,'Calvert' as County,Calvert as amount from curated_dataset.md_covid union all
select DATE,'Caroline' as County,Caroline as amount from curated_dataset.md_covid union all
select DATE,'Carroll' as County,Carroll as amount from curated_dataset.md_covid union all
select DATE,'Cecil' as County,Cecil as amount from curated_dataset.md_covid union all
select DATE,'Charles' as County,Charles as amount from curated_dataset.md_covid union all
select DATE,'Dorchester' as County,Dorchester as amount from curated_dataset.md_covid union all
select DATE,'Frederick' as County,Frederick as amount from curated_dataset.md_covid union all
select DATE,'Garrett' as County,Garrett as amount from curated_dataset.md_covid union all
select DATE,'Harford' as County,Harford as amount from curated_dataset.md_covid union all
select DATE,'Howard' as County,Howard as amount from curated_dataset.md_covid union all
select DATE,'Kent' as County,Kent as amount from curated_dataset.md_covid union all
select DATE,'Montgomery' as County,Montgomery as amount from curated_dataset.md_covid union all
select DATE,'Prince_Georges' as County,Prince_Georges as amount from curated_dataset.md_covid union all
select DATE,'Queen_Annes' as County,Queen_Annes as amount from curated_dataset.md_covid union all
select DATE,'Somerset' as County,Somerset as amount from curated_dataset.md_covid union all
select DATE,'St_Marys' as County,St_Marys as amount from curated_dataset.md_covid union all
select DATE,'Talbot' as County,Talbot as amount from curated_dataset.md_covid union all
select DATE,'Washington' as County,Washington as amount from curated_dataset.md_covid union all
select DATE,'Wicomico' as County,Wicomico as amount from curated_dataset.md_covid union all
select DATE,'Worcester' as County,Worcester as amount from curated_dataset.md_covid union all
select DATE,'Unknown' as County,Unknown as amount from curated_dataset.md_covid
 
"""

SCHEMA = [
    bigquery.SchemaField("OBJECTID", "INT64"),
    bigquery.SchemaField("DATE", "STRING"),
    bigquery.SchemaField("Allegany", "INT64"),
    bigquery.SchemaField("Anne_Arundel", "INT64"),
    bigquery.SchemaField("Baltimore", "INT64"),
    bigquery.SchemaField("Baltimore_City", "INT64"),
    bigquery.SchemaField("Calvert", "INT64"),
    bigquery.SchemaField("Caroline", "INT64"),
    bigquery.SchemaField("Carroll", "INT64"),
    bigquery.SchemaField("Cecil", "INT64"),
    bigquery.SchemaField("Charles", "INT64"),
    bigquery.SchemaField("Dorchester", "INT64"),
    bigquery.SchemaField("Frederick", "INT64"),
    bigquery.SchemaField("Garrett", "INT64"),
    bigquery.SchemaField("Harford", "INT64"),
    bigquery.SchemaField("Howard", "INT64"),
    bigquery.SchemaField("Kent", "INT64"),
    bigquery.SchemaField("Montgomery", "INT64"),
    bigquery.SchemaField("Prince_Georges", "INT64"),
    bigquery.SchemaField("Queen_Annes", "INT64"),
    bigquery.SchemaField("Somerset", "INT64"),
    bigquery.SchemaField("St_Marys", "INT64"),
    bigquery.SchemaField("Talbot", "INT64"),
    bigquery.SchemaField("Washington", "INT64"),
    bigquery.SchemaField("Wicomico", "INT64"),
    bigquery.SchemaField("Worcester", "INT64"),
    bigquery.SchemaField("Unknown", "INT64"),
]
# DAG configuration
default_args = {
    'owner': 'airflow',
    'retries': 0
}

bq_job_config = bigquery.QueryJobConfig(
    use_legacy_sql=False  # Ensure to use standard SQL (not legacy)
)

dag = DAG(
    'md_covid',
    default_args=default_args,
    description='Download CSV data and load to BigQuery',
    schedule_interval='0 20 * * *', # Change to your desired schedule, or set None to run manually
    start_date=datetime(2025, 3, 21, 20, 0),
    catchup=False
)

# Step 1: Function to download CSV file from the URL and upload it to GCS
def download_and_upload_to_gcs():
    url = 'https://opendata.maryland.gov/api/views/tm86-dujs/rows.csv?accessType=DOWNLOAD'
    response = requests.get(url)

    if response.status_code == 200:
        file_path = '/tmp/data.csv'
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_PATH)
        blob.upload_from_filename(file_path)
        os.remove(file_path)  # Clean up the local file

    else:
        raise Exception(f"Failed to download CSV from {url}")


# Step 2: Function to truncate the BigQuery table
def truncate_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Prepare the TRUNCATE TABLE SQL query
    query = f"TRUNCATE TABLE `{table_ref}`"

    # Execute the query to truncate the table
    client.query(query).result()
    print(f"Table {table_ref} truncated successfully.")

# Step 3: Load CSV from GCS to BigQuery
def load_data_to_bigquery():
    uri = f"gs://{GCS_BUCKET_NAME}/{GCS_PATH}"
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Set up BigQuery client with the correct project ID
    client = bigquery.Client(project=PROJECT_ID)

    # Load CSV into BigQuery with explicit schema and no autodetect
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skipping header row
            autodetect=False,  # Disable autodetect
            schema=SCHEMA,  # Provide the manual schema
        ),
    )
    load_job.result()  # Wait for the job to complete

    print(f"Data successfully loaded to BigQuery table: {table_ref}")

truncate_and_insert_task = BigQueryInsertJobOperator(
    task_id='truncate_and_insert_data',
    configuration={
        "query": {
            "query": truncate_and_insert_sql,
            "useLegacySql": False
        }
    },
    dag=dag,
)

# Step 4: Define the tasks in the DAG

download_task = PythonOperator(
    task_id='download_and_upload_to_gcs',
    python_callable=download_and_upload_to_gcs,
    dag=dag,
)

truncate_task = PythonOperator(
    task_id='truncate_table',
    python_callable=truncate_table,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    dag=dag,
)


# Set task dependencies
download_task >> truncate_task >> load_task >> truncate_and_insert_task
