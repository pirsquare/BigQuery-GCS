# BigQuery-GCS
[![Build Status](https://travis-ci.org/pirsquare/BigQuery-GCS.svg?branch=master)](https://travis-ci.org/pirsquare/BigQuery-GCS)
Dealing with large query results isn't so [straightforward in BigQuery](https://cloud.google.com/bigquery/querying-data#largequeryresults) . This library provides wrapper to help you execute query with large results and export it to Goolge Cloud Storage for ease of accessibility. 

1. Run your query.
2. Output results to a temporary table. 
3. Export temporary table data to GCS.
4. Delete temporary table.

## Installation

    pip install bigquery-gcs

## Examples
<pre>
from bigquery_gcs import Exporter

config = {
    'GCS_ACCESS_KEY': "YOUR_GCS_ACCESS_KEY",
    'GCS_SECRET_KEY': "YOUR_GCS_SECRET_KEY",
    'GCS_BUCKET_NAME': "YOUR_GCS_BUCKET_NAME",

    'BQ_PROJECT_ID': "YOUR_BQ_PROJECT_ID",
    'BQ_SERVICE_ACCOUNT': "YOUR_BQ_SERVICE_ACCOUNT",
    'BQ_PRIVATE_KEY_PATH': "YOUR_BQ_PRIVATE_KEY_PATH",
    'BQ_DEFAULT_QUERY_TIMEOUT': 86400,  # 24 hours
    'BQ_DEFAULT_EXPORT_TIMEOUT': 86400,  # 24 hours
}

exporter = Exporter(config)

query = "SELECT word FROM [publicdata:samples.shakespeare] LIMIT 1000"
dataset_temp = "temp"
table_temp = "shakespeare_word"
folder_name = "shakespeare" # This is your GCS folder to store result files
file_name = "shakespeare_word" # Name for exported file in GCS

# This will run query and export results to GCS
exporter.query_and_export(query, dataset_temp, table_temp, folder_name, file_name)
</pre>

