import io
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_NAME = "<project_name>"
BUCKET_NAME = "<bucket_name>"
DATASET_NAME = "<dataset_name>"
TABLE_NAME = "<table_name>"
FIXER_API_KEY = "<api_key>"

schema = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "currency_id", "type": "STRING", "mode": "REQUIRED", "maxLength": "3"},
    {"name": "rate", "type": "FLOAT", "mode": "REQUIRED"}
]

default_args = {
    "owner": "i.kyrychok",
    "depends_on_past": False,
    "email": ["my.email@mail.com"],
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes = 30),
}

def transform_to_csv(rates, date):
    """ Transform dict to CSV StringIO object """

    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["date", "currency_id", "rate"])
    for key, value in rates.items():
        writer.writerow([date, key, value])

    return output

def gcs_upload(**context):
    """ Get dict from API response, transform to CSV, upload CSV to GCS bucket """

    rates = dict(context["ti"].xcom_pull(task_ids="get_data_from_api"))
    
    gcs_hook = GCSHook(gcp_conn_id = "gcp_connection")
    gcs_hook.upload(
        bucket_name = BUCKET_NAME,
        object_name = context["csv_file"],
        data = transform_to_csv(rates, context["date"]).getvalue()
    )


with DAG(
    dag_id = "currency_rates",
    description = "Get proper currency exchange rates on every day basis. Store in Google BigQuery table",
    default_args = default_args,
    start_date = datetime(2022, 7, 26),
    schedule_interval = "@daily",
    catchup = True,
    tags = ["api", "gcs", "bigquery"]
) as dag:

    date = "{{ ds }}"
    csv_file = f"currency_rates/{date}.csv"

    get_data_from_api = SimpleHttpOperator(
        task_id = "get_data_from_api",
        http_conn_id = "fixer_api",
        endpoint = f"fixer/{date}",
        method = "GET",
        headers = {"apikey": FIXER_API_KEY},
        do_xcom_push = True,
        response_check = lambda response: response.json()["success"] == True,
        response_filter = lambda response: response.json()["rates"]
    )

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id = "create_gcs_bucket",
        gcp_conn_id = "gcp_connection", 
        project_id = PROJECT_NAME,
        bucket_name = BUCKET_NAME,
        storage_class = "STANDARD",
        location = "US",
        labels = {"owner": "airflow", "role_1": "staging", "role_2": "backup"}
    )

    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        bigquery_conn_id = "gcp_connection", 
        project_id = PROJECT_NAME,
        dataset_id=DATASET_NAME
    )

    create_bigquery_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bigquery_table",
        bigquery_conn_id = "gcp_connection",
        project_id = PROJECT_NAME,
        dataset_id = DATASET_NAME,
        table_id = TABLE_NAME,
        schema_fields = schema,
        time_partitioning = {"type": "DAY", "field": "date"},
        exists_ok = True
    )

    build_csv_and_upload_to_gcs = PythonOperator(
        task_id = "build_csv_and_upload_to_gcs",
        python_callable = gcs_upload,
        provide_context = True,
        op_kwargs = {"date": date, "csv_file": csv_file}
    )

    delete_rows_to_prevent_duplicates = BigQueryInsertJobOperator(
        task_id="delete_rows_to_prevent_duplicates",
        gcp_conn_id = "gcp_connection",
        configuration = {
            "query": {
                "query": f"DELETE FROM {PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME} WHERE date = '{date}'",
                "useLegacySql": False
            }
        }
    )

    insert_gcs_csv_data_to_bigquery = GCSToBigQueryOperator(
        task_id = "insert_gcs_csv_data_to_bigquery",
        gcp_conn_id = "gcp_connection",
        bucket = BUCKET_NAME,
        source_objects = csv_file,
        source_format = "CSV",
        destination_project_dataset_table = ".".join([PROJECT_NAME, DATASET_NAME, TABLE_NAME]),
        schema_fields=schema,
        skip_leading_rows = 1,
        max_bad_records = 0,
        write_disposition = "WRITE_APPEND"
    )   
    
    get_data_from_api >> [create_gcs_bucket, create_bigquery_dataset] >> create_bigquery_table >> [build_csv_and_upload_to_gcs, delete_rows_to_prevent_duplicates] >> insert_gcs_csv_data_to_bigquery


### ToDo: Add second backup to postgres or cheaper bucket
