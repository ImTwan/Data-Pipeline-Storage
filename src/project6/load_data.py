import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
import os


# -------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -------------------------------------------------
# AUTHENTICATION (Service Account)
# -------------------------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"D:\python try hard\unigap\project6\data\json_key\fresh-ocean-475916-m2-d87215690697.json"
)

# -------------------------------------------------
# FUNCTION: Load JSONL from GCS → BigQuery
# -------------------------------------------------
def load_jsonl_to_bigquery(project_id, dataset_id, table_id, gcs_url, schema):
    """
    Loads a JSONL file stored in Google Cloud Storage (GCS)
    into a BigQuery table.

    Parameters:
        project_id (str): Your Google Cloud project ID.
        dataset_id (str): The dataset where the table belongs.
        table_id (str): Name of the table to load data into.
        gcs_url (str): Path to the .jsonl file in GCS.
        schema (list): BigQuery schema of the destination table.
    """
    
    try:
        client = bigquery.Client(project=project_id)

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrite table
            autodetect=False, # Schema is manually defined
        )

        # Logging
        logging.info("-------------------------------------------------")
        logging.info(f"📌 STARTING LOAD JOB")
        logging.info(f"➡ Table: {table_ref}")
        logging.info(f"➡ File:  {gcs_url}")
        logging.info("-------------------------------------------------")

        load_job = client.load_table_from_uri(
            gcs_url, table_ref, job_config=job_config
        )

        load_job.result()  # Wait for job to finish

        logging.info(f"✅ Success: Loaded data into {table_ref}")

        # Fetch row count
        table = client.get_table(table_ref)
        logging.info(f"📊 Total rows in table: {table.num_rows}")

    except Exception as e:
        logging.error(f"❌ ERROR loading into BQ ({table_id}): {str(e)}")
        raise e


# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------
project_id = "fresh-ocean-475916-m2"
dataset_id = "glamira_dataset"

# -------------------------------------------------
# SCHEMAS
# -------------------------------------------------
ip_location_schema = [
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("city", "STRING"),
]

product_ids_to_crawl_schema = [
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("url", "STRING"),
]

crawl_product_ids_schema = [
    bigquery.SchemaField("product_id", "INTEGER"),
]

# -------------------------------------------------
# LOAD THREE TABLES
# -------------------------------------------------
load_jsonl_to_bigquery(
    project_id,
    dataset_id,
    "ip_locations",
    "gs://twan_glamira/dataset_export/ip_location_results.jsonl",
    ip_location_schema
)

load_jsonl_to_bigquery(
    project_id,
    dataset_id,
    "product_ids_to_crawl",
    "gs://twan_glamira/dataset_export/product_ids_to_crawl.jsonl",
    product_ids_to_crawl_schema
)

load_jsonl_to_bigquery(
    project_id,
    dataset_id,
    "crawl_product_id",
    "gs://twan_glamira/dataset_export/valid_product_ids.jsonl",
    crawl_product_ids_schema
)
