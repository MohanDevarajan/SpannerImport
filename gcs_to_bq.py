def load_table_uri_csv(table_id):

    # [START bigquery_load_table_gcs_csv]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "etl-pipeline-307118.staging.STG_FACILITY_X_PARTY"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("KEY_VALUE", "INTEGER"),
            bigquery.SchemaField("ARR_ID", "INTEGER"),
            bigquery.SchemaField("Snapshot_DATE", "DATE"),
            bigquery.SchemaField("Snapshot_Date_ID", "INTEGER"),
            bigquery.SchemaField("Limit_Value", "NUMERIC"),
            bigquery.SchemaField("Limit_GBP", "NUMERIC"),
            bigquery.SchemaField("Balance", "NUMERIC"),
            bigquery.SchemaField("Balance_GBP", "NUMERIC"),
            bigquery.SchemaField("BI_INSERT_DATE", "DATE"),
            bigquery.SchemaField("BI_UPDATE_DATE", "DATE"),
            bigquery.SchemaField("Undrawn_amount", "NUMERIC"),
            bigquery.SchemaField("Undrawn_amount_GBP", "NUMERIC"),
            bigquery.SchemaField("Cleared_Balance", "NUMERIC"),
            bigquery.SchemaField("Cleared_Balance_GBP", "NUMERIC"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://rvsrdldemo20/stg_trading_trans.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    # [END bigquery_load_table_gcs_csv]

table = "stg"
load_table_uri_csv(table)
