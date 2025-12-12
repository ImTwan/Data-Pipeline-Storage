import logging
import os
from flask import Request
from google.cloud import bigquery

def trigger_bigquery_load_GCP(request: Request):
    """
    Cloud Run function to load JSONL files from GCS into BigQuery.
    Triggered by Cloud Storage finalized event.
    """

    try:
        # -----------------------------
        # 1️⃣ Parse JSON body
        # -----------------------------
        data = request.get_json(silent=True)
        if not data:
            logging.error("No JSON body received")
            return ("Bad Request: No JSON body", 400)

        file_name = data.get("name")
        bucket_name = data.get("bucket")

        if not file_name or not bucket_name:
            logging.error("Missing file name or bucket name")
            return ("Bad Request: Missing fields", 400)

        logging.info(f"Triggered by file: {file_name}")
        logging.info(f"Bucket: {bucket_name}")

        # -----------------------------
        # 2️⃣ Map file prefix to BigQuery table
        # -----------------------------
        TABLE_MAP = {
            "ip_location_results": "ip_locations",
            "product_ids_to_crawl": "product_ids_to_crawl",
            "valid_product_ids": "crawl_product_id"
        }

        # Extract the prefix from the filename
        filename_only = os.path.basename(file_name).strip()  # remove spaces
        prefix = os.path.splitext(filename_only)[0].lower()  # convert to lowercase
        table_name = TABLE_MAP.get(prefix)

        if not table_name:
            logging.warning(f"No table mapped for file prefix: {prefix}. Skipping.")
            return ("Ignored", 200)

        logging.info(f"Mapped to BigQuery table: {table_name}")

        # -----------------------------
        # 3️⃣ Load file into BigQuery
        # -----------------------------
        bq = bigquery.Client()
        dataset = "glamira_dataset"  # change this to your dataset name
        uri = f"gs://{bucket_name}/{file_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        logging.info(f"Starting BigQuery load from {uri} to {dataset}.{table_name}")
        load_job = bq.load_table_from_uri(
            uri,
            f"{dataset}.{table_name}",
            job_config=job_config
        )

        load_job.result()  # wait for job to complete
        logging.info(f"BigQuery load completed for table: {table_name}")

        return ("OK", 200)

    except Exception as e:
        logging.exception("Cloud Function failed")
        return (f"Error: {str(e)}", 500)
