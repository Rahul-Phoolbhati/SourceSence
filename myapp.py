import asyncio
import json
import os
import time
from typing import Any, Dict

# Import SDK components
from application_sdk.activities.metadata_extraction.sql import BaseSQLMetadataExtractionActivities
from application_sdk.clients.sql import BaseSQLClient
from application_sdk.clients.utils import get_workflow_client
from application_sdk.handlers.sql import BaseSQLHandler
from application_sdk.worker import Worker
from application_sdk.workflows.metadata_extraction.sql import BaseSQLMetadataExtractionWorkflow
from application_sdk.observability.logger_adaptor import get_logger

APPLICATION_NAME = "postgres-app-example" # Define application name
logger = get_logger(__name__)

# --- Custom Component Definitions (as shown in previous sections) ---
class SQLClient(BaseSQLClient):
    DB_CONFIG = {
        "template": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}",
        "required": ["username", "password", "host", "port", "database"],
    }

class SampleSQLActivities(BaseSQLMetadataExtractionActivities):
    fetch_database_sql = "SELECT datname as database_name FROM pg_database WHERE datname = current_database();"
    fetch_schema_sql = "SELECT s.* FROM information_schema.schemata s WHERE s.schema_name NOT LIKE 'pg_%' AND s.schema_name != 'information_schema' AND concat(s.CATALOG_NAME, concat('.', s.SCHEMA_NAME)) !~ '{normalized_exclude_regex}' AND concat(s.CATALOG_NAME, concat('.', s.SCHEMA_NAME)) ~ '{normalized_include_regex}';"
    fetch_table_sql = "SELECT t.* FROM information_schema.tables t WHERE concat(current_database(), concat('.', t.table_schema)) !~ '{normalized_exclude_regex}' AND concat(current_database(), concat('.', t.table_schema)) ~ '{normalized_include_regex}' {temp_table_regex_sql};"
    extract_temp_table_regex_table_sql = "AND t.table_name !~ '{exclude_table_regex}'"
    extract_temp_table_regex_column_sql = "AND c.table_name !~ '{exclude_table_regex}'"
    fetch_column_sql = "SELECT c.* FROM information_schema.columns c WHERE concat(current_database(), concat('.', c.table_schema)) !~ '{normalized_exclude_regex}' AND concat(current_database(), concat('.', c.table_schema)) ~ '{normalized_include_regex}' {temp_table_regex_sql};"

class SampleSQLWorkflowHandler(BaseSQLHandler):
    tables_check_sql = "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES t WHERE concat(TABLE_CATALOG, concat('.', TABLE_SCHEMA)) !~ '{normalized_exclude_regex}' AND concat(TABLE_CATALOG, concat('.', TABLE_SCHEMA)) ~ '{normalized_include_regex}' AND TABLE_SCHEMA NOT IN ('performance_schema', 'information_schema', 'pg_catalog', 'pg_internal') {temp_table_regex_sql};"
    temp_table_regex_sql = "AND t.table_name !~ '{exclude_table_regex}'"
    metadata_sql = "SELECT schema_name, catalog_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'"
# --- End Custom Component Definitions ---

def save_metadata_to_json(workflow_id: str, run_id: str, output_dir: str = "./local/dapr/objectstore/artifacts") -> None:
    """
    Collect and save extracted metadata to a JSON file in the main directory.
    
    Args:
        workflow_id: The workflow ID
        run_id: The workflow run ID
        output_dir: Directory where artifacts are stored
    """
    try:
        # Path to the transformed data
        transformed_dir = f"{output_dir}/apps/default/workflows/{workflow_id}/{run_id}/transformed"
        
        metadata = {
            "workflow_info": {
                "workflow_id": workflow_id,
                "run_id": run_id,
                "extraction_timestamp": time.time()
            },
            "extracted_metadata": {}
        }
        
        # Collect metadata from different asset types
        asset_types = ["table", "database", "schema", "column"]
        
        for asset_type in asset_types:
            asset_dir = f"{transformed_dir}/{asset_type}"
            if os.path.exists(asset_dir):
                asset_files = [f for f in os.listdir(asset_dir) if f.endswith('.json')]
                asset_data = []
                
                for file in asset_files:
                    file_path = os.path.join(asset_dir, file)
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                asset_data.extend(data)
                            else:
                                asset_data.append(data)
                    except Exception as e:
                        logger.warning(f"Error reading {file_path}: {e}")
                
                metadata["extracted_metadata"][asset_type] = asset_data
                logger.info(f"Collected {len(asset_data)} {asset_type} records")
        
        # Save to JSON file in main directory
        output_file = f"extracted_metadata_{workflow_id}_{int(time.time())}.json"
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Metadata saved to {output_file}")
        logger.info(f"Total records: {sum(len(records) for records in metadata['extracted_metadata'].values())}")
        
    except Exception as e:
        logger.error(f"Error saving metadata to JSON: {e}")

async def run_sql_application(daemon: bool = True) -> Dict[str, Any]:
    """Sets up and runs the SQL metadata extraction workflow."""
    logger.info(f"Starting SQL application: {APPLICATION_NAME}")

    # 1. Initialize workflow client (uses Temporal client configured via constants)
    workflow_client = get_workflow_client(application_name=APPLICATION_NAME)
    await workflow_client.load()

    # 2. Instantiate activities with custom SQL client and handler
    activities = SampleSQLActivities(
        sql_client_class=SQLClient, # Use our custom PostgreSQL client
        handler_class=SampleSQLWorkflowHandler # Use our custom handler
    )

    # 3. Set up the Temporal worker
    #    - Uses the base workflow class (handles orchestration)
    #    - Registers the activities instance (provides extraction logic)
    worker = Worker(
        workflow_client=workflow_client,
        workflow_classes=[BaseSQLMetadataExtractionWorkflow],
        workflow_activities=BaseSQLMetadataExtractionWorkflow.get_activities(activities),
    )

    # 4. Configure workflow arguments
    #    These arguments control credentials, connection details, filtering, etc.
    #    They are typically loaded from a secure source or environment variables.
    workflow_args = {
        "credentials": {
            "authType": "basic",
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "username": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "database": os.getenv("POSTGRES_DATABASE", "postgres2"),
        },
        "connection": {
            "connection_name": "test-postgres-connection", # Example connection name
            "connection_qualified_name": f"default/postgres/{int(time.time())}", # Example qualified name
        },
        "metadata": {
            # Define include/exclude filters (regex patterns)
            "exclude-filter": '{"postgres2":["^pg_.*"]}',
            "include-filter": "{}",
            # Define regex for temporary tables to exclude
            "temp-table-regex": "^temp_",
            # Extraction method (direct connection)
            "extraction-method": "direct",
            # Options to exclude views or empty tables
            "exclude_views": "false",
            "exclude_empty_tables": "true",
        },
        "tenant_id": os.getenv("ATLAN_TENANT_ID", "default"), # Tenant ID from constants
        # --- Optional arguments ---
        # "workflow_id": "existing-workflow-run_id", # Uncomment to rerun a specific workflow
        # "cron_schedule": "0 */1 * * *", # Uncomment to run hourly
    }

    # 5. Start the workflow execution via the Temporal client
    logger.info(f"Starting workflow with args: {workflow_args}")
    workflow_response = await workflow_client.start_workflow(
        workflow_args, BaseSQLMetadataExtractionWorkflow
    )
    logger.info(f"Workflow started: {workflow_response}")

    # 6. Start the Temporal worker to process tasks
    #    Set daemon=True to run in background, False to run in foreground (for scripts)
    logger.info(f"Starting worker (daemon={daemon})...")
    await worker.start(daemon=daemon)

    return workflow_response # Returns info like workflow_id, run_id

# --- Script Execution ---
if __name__ == "__main__":
    # Run the application in the foreground when script is executed directly
    asyncio.run(run_sql_application(daemon=False))
