import asyncio

from app.activities import HelloWorldActivities, MyWorkflowActivities

from application_sdk.workflows.metadata_extraction.sql import BaseSQLMetadataExtractionWorkflow
from application_sdk.activities.metadata_extraction.sql import BaseSQLMetadataExtractionActivities
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
import asyncio
from datetime import timedelta
import time
from typing import Any, Callable, Coroutine, Dict, List, Sequence

from app.activities import HelloWorldActivities, MyWorkflowActivities
from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces
from application_sdk.workflows import WorkflowInterface
from temporalio import workflow

logger = get_logger(__name__)
workflow.logger = logger
metrics = get_metrics()
traces = get_traces()

APPLICATION_NAME = "hello-world"



from application_sdk.handlers.sql import BaseSQLHandler        
from application_sdk.clients.sql import BaseSQLClient
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


# --- Custom Component Definitions (as shown in previous sections) ---
class SQLClient(BaseSQLClient):
    DB_CONFIG = {
        "template": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}",
        "required": ["username", "password", "host", "port", "database"],
    }


import os

@workflow.defn
class HelloWorldWorkflow(BaseSQLMetadataExtractionWorkflow):
    @observability(logger=logger, metrics=metrics, traces=traces)
    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> None:
        """
        This workflow is used to say hello to a name.

        Args:
            workflow_config (Dict[str, Any]): The workflow configuration

        Returns:
            None
        """

        # Get the workflow configuration from the state store
       
        workflow_args = {
        "credentials": {
            "authType": "basic",
            "host": "localhost",
            "port": "5432",
            "username": "postgres",
            "password": "password",
            "database": "postgres2",
        },
        "connection": {
            "connection_name": "test-postgres-connection", # Example connection name
            "connection_qualified_name": f"default/postgres/{int(1)}", # Example qualified name
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
        "tenant_id": "default", # Tenant ID from constants
        # --- Optional arguments ---
        # "workflow_id": "existing-workflow-run_id", # Uncomment to rerun a specific workflow
        # "cron_schedule": "0 */1 * * *", # Uncomment to run hourly
    }

        # logger.info("Starting hello world workflow")

        activities = SampleSQLActivities(
            sql_client_class=SQLClient, # Use our custom PostgreSQL client
            handler_class=SampleSQLWorkflowHandler # Use our custom handler
        ) 
        
        activities = self.get_activities(activities)

        # Wait for all activities to complete
        await asyncio.gather(*activities)
        


        logger.info("Hello world workflow completed")

    @staticmethod
    def get_activities(activities: ActivitiesInterface) -> Sequence[Callable[..., Any]]:
        """Get the sequence of activities to be executed by the workflow.

        Args:
            activities (ActivitiesInterface): The activities instance
                containing the hello world operations.

        Returns:
            Sequence[Callable[..., Any]]: A sequence of activity methods to be executed
                in order.
        """
        if not isinstance(activities, SampleSQLActivities):
            raise TypeError("Activities must be an instance of HelloWorldActivities")

        return BaseSQLMetadataExtractionWorkflow.get_activities(activities)
        
        




@observability(logger=logger, metrics=metrics, traces=traces)
async def main():
    logger.info("Starting hello world application")
    # initialize application
    app = BaseApplication(name=APPLICATION_NAME)

    # setup workflow
    await app.setup_workflow(
        workflow_and_activities_classes=[(HelloWorldWorkflow, SampleSQLActivities)],
    )
    
    # app.start_workflow(workflow_args={"metadata": "John Doe"})

    # start worker
    await app.start_worker()

    # Setup the application server
    await app.setup_server(workflow_class=HelloWorldWorkflow)
    

    # start server
    await app.start_server()


if __name__ == "__main__":
    asyncio.run(main())

