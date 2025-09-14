import asyncio
import json
import os
import time
from typing import Any, Dict

# Import SDK components
from application_sdk.activities.metadata_extraction.sql import BaseSQLMetadataExtractionActivities
from application_sdk.clients.sql import BaseSQLClient
from application_sdk.handlers.sql import BaseSQLHandler
from application_sdk.workflows.metadata_extraction.sql import BaseSQLMetadataExtractionWorkflow
from application_sdk.application import BaseApplication
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces

logger = get_logger(__name__)
metrics = get_metrics()
traces = get_traces()

APPLICATION_NAME = "postgres-app-example"

# --- Custom Component Definitions ---
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

# --- Custom Activities with JSON Saving ---
class SQLMetadataActivities(BaseSQLMetadataExtractionActivities):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow_id = None
        self.run_id = None

    def set_workflow_info(self, workflow_id: str, run_id: str):
        """Set workflow information for JSON saving."""
        self.workflow_id = workflow_id
        self.run_id = run_id

    def save_metadata_to_json(self, workflow_id: str, run_id: str, output_dir: str = "./local/dapr/objectstore/artifacts") -> None:
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
                    file = f"chunk-0-part1.json"
                    asset_data = []
                    
                    file_path = os.path.join(asset_dir, file)
                    try:
                        with open(file_path, 'r') as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue  # Skip empty lines
                                try:
                                    obj = json.loads(line)
                                    asset_data.append(obj)
                                except json.JSONDecodeError as je:
                                    logger.warning(f"Skipping invalid JSON line in {file_path}: {je}")
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

# --- Custom Workflow with JSON Saving ---
class SQLMetadataWorkflow(BaseSQLMetadataExtractionWorkflow):
    def __init__(self):
        super().__init__()
        self.activities_instance = None

    async def run(self, workflow_config: Dict[str, Any]) -> None:
        """Run the SQL metadata extraction workflow with JSON saving."""
        logger.info("Starting SQL metadata extraction workflow")
        
        # Set workflow info for activities
        if self.activities_instance:
            self.activities_instance.set_workflow_info(
                workflow_config.get("workflow_id", "unknown"),
                workflow_config.get("run_id", "unknown")
            )
        
        # Call parent workflow
        await super().run(workflow_config)
        
        # Save metadata to JSON after workflow completion
        workflow_id = workflow_config.get("workflow_id", "unknown")
        run_id = workflow_config.get("run_id", "unknown")
        
        if self.activities_instance:
            self.activities_instance.save_metadata_to_json(workflow_id, run_id)
        
        logger.info("SQL metadata extraction workflow completed with JSON saving")

@observability(logger=logger, metrics=metrics, traces=traces)
async def main():
    """Main function to start the SQL metadata extraction server."""
    logger.info(f"Starting SQL metadata extraction application: {APPLICATION_NAME}")
    
    # Initialize application
    app = BaseApplication(name=APPLICATION_NAME)
    
    # Create activities instance
    activities_instance = SQLMetadataActivities(
        sql_client_class=SQLClient,
        handler_class=SampleSQLWorkflowHandler
    )
    
    # Setup workflow with custom activities
    await app.setup_workflow(
        workflow_and_activities_classes=[(SQLMetadataWorkflow, activities_instance)],
    )
    
    # Start worker
    await app.start_worker()
    
    # Setup the application server
    await app.setup_server(workflow_class=SQLMetadataWorkflow)
    
    # Start server
    await app.start_server()

if __name__ == "__main__":
    asyncio.run(main())
