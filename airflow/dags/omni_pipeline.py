from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import os

DEFAULT_ARGS = {
    "owner": "Abulkhayr",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["abdussamadlawal7@gmail.com"],
}

bronze_ingestion = DatabricksRunNowOperator(
    task_id="bronze_ingestion",
    databricks_conn_id="databricks_default",
    job_id=int(os.environ.get("DATABRICKS_JOB_BRONZE", 0)),
)

market_ingestion = DatabricksRunNowOperator(
    task_id="market_data_ingestion",
    databricks_conn_id="databricks_default",
    job_id=int(os.environ.get("DATABRICKS_JOB_MARKET", 0)),
)

paye_payroll = DatabricksRunNowOperator(
    task_id="paye_payroll",
    databricks_conn_id="databricks_default",
    job_id=int(os.environ.get("DATABRICKS_JOB_PAYE", 0)),
)

dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /workspaces/omni/Omni_dbt && dbt run --profiles-dir .",
    )

dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /workspaces/omni/Omni_dbt && dbt test --profiles-dir .",
    )

bronze_ingestion >> market_ingestion
market_ingestion >> paye_payroll
paye_payroll >> dbt_run
dbt_run >> dbt_test