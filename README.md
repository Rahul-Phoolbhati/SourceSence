# SourceSense

SourceSense is a metadata extraction application built on the [Atlan Apps Framework](https://github.com/atlanhq/application-sdk).  
It connects to a PostgreSQL database, extracts schema-level metadata, and exports it in a structured JSON format.  
The project demonstrates how to transform raw database structures into meaningful metadata that can be used for data discovery, governance, and lineage.

---

## Features

- *PostgreSQL Integration*  
  Connects securely to PostgreSQL databases using standard credentials.

- *Metadata Extraction*  
  - Databases, schemas, tables, and columns  
  - Data types and constraints (primary keys, foreign keys, not nulls)  
  - Business context from PostgreSQL COMMENT metadata  
  - Optional lineage from foreign key relationships  
  - Optional data quality metrics such as null counts and distinct values

- *Metadata Export*  
  Extracted metadata is consolidated and written to a single JSON file for reuse or integration.

- *Extensible Design*  
  The architecture is modular, with reusable SQL activities and workflow handlers, enabling extension to other SQL-based sources.

---

## Architecture

The application follows a layered design:

1. *Connector Layer*  
   - A custom SQLClient extends BaseSQLClient.  
   - Defines the PostgreSQL connection template (postgresql+psycopg://...).  
   - Supports credentials such as host, port, user, password, and database.

2. *Metadata Processing Layer*  
   - SampleSQLActivities contains queries for extracting metadata from PostgreSQL system catalogs (information_schema and pg_catalog).  
   - Supports include/exclude filters and regex patterns for excluding temporary tables.  

3. *Workflow Layer*  
   - SampleSQLWorkflowHandler manages orchestration of extraction.  
   - Built on BaseSQLMetadataExtractionWorkflow from the Atlan SDK.  
   - Configurable with connection details, include/exclude filters, and regex-based rules.

4. *Export Layer*  
   - save_metadata_to_json() collects workflow outputs for asset types (databases, schemas, tables, columns).  
   - Merges them into a single JSON file saved in the root directory.  
   - File naming convention:  
     
     extracted_metadata_<workflow_id>_<timestamp>.json
     

5. *Orchestration and Execution*  
   - run_sql_application() initializes the Temporal workflow client.  
   - Starts the worker, registers activities, and launches workflows.  
   - Supports both daemon (background) and foreground execution modes.  

---

## Setup Instructions

### Prerequisites
- Python 3.11 or later  
- PostgreSQL instance (local or remote)  
- Atlan App SDK installed
- Temporal and Dapr setup 

### Installation Guides
- [macOS Setup Guide](https://github.com/atlanhq/application-sdk/blob/main/docs/docs/setup/MAC.md)
- [Linux Setup Guide](https://github.com/atlanhq/application-sdk/blob/main/docs/docs/setup/LINUX.md)
- [Windows Setup Guide](https://github.com/atlanhq/application-sdk/blob/main/docs/docs/setup/WINDOWS.md)


### Installation
Clone the repository and install dependencies:

```bash
git clone https://github.com/Rahul-Phoolbhati/SourceSence/tree/main
cd SourceSense

# Install UV
curl -LsSf https://astral.sh/uv/0.7.3/install.sh | sh

# Install Python 3.11.10
uv venv --python 3.11.10

# activate the venv
source .venv/bin/activate

# Python dependencies
uv run poe download-components

# Run the Workflow for extraction of metadata
uv run myapp.py

# Save the Extraceted data into single json file (from Dapr object store)
uv run save_metadata.py
```


**Access the application:**
- **Temporal UI**: http://localhost:8233



### Project Structure
```
SourceSence/
├── experiments/        # Experimental work
├── local/              # Local data storage
├── myapp.py            # Application entry point
├── pyproject.toml     # Dependencies and config
└── README.md          # This file
```

## Learning Resources
- [Temporal Documentation](https://docs.temporal.io/)
- [Atlan Application SDK Documentation](https://github.com/atlanhq/application-sdk/tree/main/docs)
- [Python FastAPI Documentation](https://fastapi.tiangolo.com/)

## Contributing
We welcome contributions! Please feel free to submit a Pull Request.