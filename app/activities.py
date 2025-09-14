import asyncio
from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from temporalio import activity
from myapp import run_sql_application

logger = get_logger(__name__)
activity.logger = logger


class HelloWorldActivities(ActivitiesInterface):
    @activity.defn
    async def say_hello(self, name: str) -> str:
        logger.info(f"Saying hello to {name}")
        return f"Hello, {name}!"

    @activity.defn
    def say_hello_sync(self, name: str) -> str:
        logger.info(f"Saying hello to {name}")
        return f"Hello, {name}!"


class MyWorkflowActivities(ActivitiesInterface):
    @activity.defn
    async def run_extraction(self, name: str) -> str:
        logger.info(f"Running extraction for {name}")
        asyncio.run(run_sql_application(daemon=False))
        return f"Extraction completed for {name}!"