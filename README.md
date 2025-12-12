# Project 06: Data Pipeline & Storage
## 1. Project Structure
<pre>
project-6/
├── logs/
├── src/
│   └── project6/
│       ├── export.py
│       ├── load_data.py
│       ├── trigger_bigquery_test_on_GCP.py
│       └── trigger_bigquery_test_on_local.py
├── .gitignore
├── .python-version
├── README.md
├── pyproject.toml
└── uv.lock
</pre>
## 2. Installation & Environment Setup
* Install uv:
```text
 pip install uv
```
* Install dependencies:
```text
 uv sync
```

## 3. Usage
From project root:
* Export MongoDB → GCS
```text
 uv run src/project6/export.py
```
* Load GCS → BigQuery manually
```text
 uv run src/project6/load_data.py
```
* Trigger BigQuery from local test
```text
 uv run src/project6/trigger_bigquery_test_on_local.py
```

## 4. Project Overview
This project is a continuation of Project 5: Data Collection, Storage, and Foundation (link to project: https://github.com/ImTwan/Project-05-Data-Collection-Storage-Foundation?tab=readme-ov-file#data-collection-storage-foundation). The deliverables of this project are:
* Automated data pipeline
* Cloud Function triggers
* BigQuery tables
* Monitoring setup
Both project follows the workflow


### 4.1. Data Export Process


