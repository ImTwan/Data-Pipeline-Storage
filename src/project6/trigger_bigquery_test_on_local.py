import logging
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)

# -------------------------------------------------
# CLOUD FUNCTION ENTRY POINT
# -------------------------------------------------
def trigger_bigquery_load(event, context):
    """
    Triggered by a new file upload to GCS.
    1. Detects the new file
    2. Starts a BigQuery load job
    3. Logs the results, including context metadata
    """

    # -------------------------------
    # 1. Detect new file in GCS
    # -------------------------------
    file_name = event['name']
    bucket_name = event['bucket']
    gcs_url = f"gs://{bucket_name}/{file_name}"

    logging.info("📥 New file detected in GCS")
    logging.info(f"📁 Bucket: {bucket_name}")
    logging.info(f"📄 File:   {file_name}")
    logging.info(f"🆔 Event ID: {context.event_id}")
    logging.info(f"🕒 Timestamp: {context.timestamp}")
    logging.info(f"📌 Resource: {context.resource}")

    # -------------------------------
    # 2. Schema detection
    # -------------------------------
    if "ip_location" in file_name:
        table_id = "ip_locations"
        schema = [
            bigquery.SchemaField("ip", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("city", "STRING"),
        ]
    elif "product_ids_to_crawl" in file_name:
        table_id = "product_to_crawl"
        schema = [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING"),
        ]
    elif "valid_product_ids" in file_name:
        table_id = "crawl_product_id"
        schema = [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        ]
    else:
        logging.warning("❌ File name does not match any known schema. Skipping.")
        return

    try:
        # --------------------------------------------
        # 3. LOCAL SERVICE ACCOUNT CREDENTIALS SUPPORT
        # --------------------------------------------
        sa_path = r"D:\python try hard\unigap\project6\data\json_key\fresh-ocean-475916-m2-d87215690697.json"

        if os.path.exists(sa_path):
            logging.info("🔑 Using LOCAL service account credentials")
            credentials = service_account.Credentials.from_service_account_file(sa_path)
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        else:
            logging.info("☁ Using Cloud Function default credentials")
            client = bigquery.Client()

        dataset_id = "glamira_dataset"
        table_ref = f"{client.project}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = client.load_table_from_uri(gcs_url, table_ref, job_config=job_config)
        load_job.result()  # Wait for completion

        # -------------------------------
        # 4. Log results
        # -------------------------------
        logging.info("✅ BigQuery load job completed successfully")
        logging.info(f"📌 Table: {table_ref}")
        logging.info(f"📦 Source: {gcs_url}")

        table = client.get_table(table_ref)
        logging.info(f"📊 Total rows loaded: {table.num_rows}")

    except Exception as e:
        logging.error(f"❌ Error loading file into BigQuery: {str(e)}")
        raise e


# -------------------------------------------------
# LOCAL TESTING ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    # Files inside your GCS bucket folder
    test_files = [
        "dataset_export/ip_location_results.jsonl",
        "dataset_export/product_ids_to_crawl.jsonl",
        "dataset_export/valid_product_ids.jsonl",
    ]

    bucket = "twan_glamira"

    # Fake context object for local testing
    class FakeContext:
        event_id = "test-event-id"
        timestamp = "2025-12-09T00:00:00Z"
        resource = {"service": "storage.googleapis.com", "name": bucket}

    context = FakeContext()

    # Run test for each file
    for f in test_files:
        print("\n============================")
        print(f"🔥 Testing file: {f}")
        print("============================")

        event = {
            "bucket": bucket,
            "name": f,
        }

        trigger_bigquery_load(event, context)
