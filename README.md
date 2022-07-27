# Deployment

1. Build and run standalone airflow docker container: 
```
docker build -t airflow-local . && docker run -it -p 8080:8080 airflow-local airflow standalone
```

2. Create http connection to https://api.apilayer.com. Name it "*fixer_api*".

3. Create Google Cloud Platform connection. Use json key of service account with write permissions to Google Cloud Storage and BigQuery. Name it "*gcp_connection*".

4. Set variables inside the dag script ("*currency_rates.py*"):
```
    PROJECT_NAME - your Google Cloud project name
    BUCKET_NAME - GCS bucket (will be created if not exists)
    DATASET_NAME - BigQuery dataset name (will be created if not exists)
    TABLE_NAME - BigQuery table name (will be created if not exists)
    FIXER_API_KEY - api.apilayer.com API key
```
