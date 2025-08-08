import os
from datetime import datetime


from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import TestBehavior


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline",)
project_dir="/usr/local/airflow/dags/dbt/data_pipeline"
execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",)
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline",),
    operator_args={
        "install_deps": True,
        "defer_tests": True 
        },
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_ALL
        ),
    schedule="30 12 * * *",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
