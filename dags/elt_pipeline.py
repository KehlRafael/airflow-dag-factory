import pendulum
import logging
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from factory.dag import success_callback, failure_callback

logger = logging.getLogger()

dag_args = {
    "default_args" : {
        "start_date": datetime(2024, 1, 1, tzinfo=pendulum.timezone("America/Sao_Paulo")),
        "owner": "DAG Dev Team"
    },
    "tags": ["elt"],
    "schedule": "*/1 * * * *", # Every minute
    "catchup": False,
}


def check_remote(**kwargs) -> str:
    """Simulates a function that checks a remote file system for new files"""
    from random import random
    if random() > 0.7:
        logger.info("No new data in the remote repository!")
        return "no_data"
    logger.info("New data found! Starting extraction.")
    return "new_data.extract_data"


def extract_from_remote(**kwargs) -> int:
    """Simulates a function that extracts data from a remote file system"""
    from random import randint
    files_found = randint(1, 100)
    logger.info(f"Copying [{files_found}] file(s) from remote to Data Lake.")
    logger.info(f"All [{files_found}] files moved successfuly!")
    return files_found


def load_data_to_warehouse(**kwargs) -> None:
    """Simulates a function that loads data from a lake to a DWH"""
    files_found = kwargs["files_found"]
    for i in range(files_found):
        logger.info(f"Loading file {i+1}/{files_found}")
    logger.info("Done! All files loaded into the Data Warehouse.")


def run_data_transformation(**kwargs) -> None:
    """Simulates a function that runs transformations to data inside the DWH"""
    logger.info("Remember: Airflow is an orchestrator! Transformations are not done by the worker.")
    logger.info("Also notice that files went from a remote file system to a data lake. Nothing in the workers disk or memory!")
    logger.info("Transformations done by the Data Warehouse!")


# DAG definition
with DAG("elt_pipeline", **dag_args) as dag:

    begin = EmptyOperator(
        task_id="begin",
        on_success_callback=success_callback
    )

    check_for_data = BranchPythonOperator(
        task_id="check_remote_for_data",
        python_callable=check_remote,
        on_failure_callback=failure_callback
    )

    no_data = EmptyOperator(
        task_id="no_data"
    )

    new_data = TaskGroup(
        group_id="new_data"
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_remote,
        on_failure_callback=failure_callback,
        task_group=new_data
    )

    load_data = PythonOperator(
        task_id="load_data",
        op_kwargs={
            "files_found": extract_data.output
        },
        python_callable=load_data_to_warehouse,
        on_failure_callback=failure_callback,
        task_group=new_data
    )

    apply_transformation = PythonOperator(
        task_id="apply_transformation",
        python_callable=run_data_transformation,
        on_failure_callback=failure_callback,
        task_group=new_data
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=success_callback,
        trigger_rule='none_failed'
    )

extract_data >> load_data >> apply_transformation
begin >> check_for_data >> [no_data, new_data] >> end
