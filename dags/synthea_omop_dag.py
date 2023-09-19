"""

## Run ELT on energy capacity data with Cosmos and dbt Core

Shows how to use the Cosmos, to create an Airflow task group from a dbt project.
The data is loaded into a database and analyzed using the Astro Python SDK. 
"""

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro.files import File
from pendulum import datetime
import pandas as pd
import logging
import os

task_logger = logging.getLogger("airflow.task")


CONNECTION_ID = "db_conn"
DB_NAME = "postgres"
SCHEMA_NAME = "postgres"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/ETL-Synthea-dbt"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    start_date=datetime(2023, 3, 26),
    schedule=None,
    catchup=False,
)
def synthea_omop_dag():

    # use the DbtTaskGroup class to create a task group containing task created
    # from dbt models
    dbt_tg = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"country_code": "CH"}',
        },
    )

    (
        dbt_tg
        
    )


synthea_omop_dag()
