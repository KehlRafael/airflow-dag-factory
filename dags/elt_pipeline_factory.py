import logging

from factory.dag import GeneralDAGFactory

logger = logging.getLogger()

dag_args = {
    "tags": ["elt"],
    "schedule": "*/1 * * * *" # Every minute
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


tasks = [
    {
        "task_id": "check_for_data",
        "python_callable": check_remote,
        "operator": "BranchPython",
        "dependencies": []
    },{
        "task_id": "no_data",
        "operator": "Empty",
        "dependencies": ["check_for_data"]
    },{
        "group_id": "new_data",
        "operator": "TaskGroup",
        "dependencies": ["check_for_data"]
    },{
        "task_id": "extract_data",
        "python_callable": extract_from_remote,
        "task_group": "new_data",
        "operator": "Python",
        "dependencies": []
    },{
        "task_id": "load_data",
        "python_callable": load_data_to_warehouse,
        "task_group": "new_data",
        "op_kwargs": {
            "files_found": "{{ ti.xcom_pull(task_ids='new_data.extract_data' , key='return_value') }}"
        },
        "operator": "Python",
        "dependencies": ["extract_data"]
    },{
        "task_id": "apply_transformation",
        "python_callable": run_data_transformation,
        "task_group": "new_data",
        "operator": "Python",
        "dependencies": ["load_data"]
    }
]

dag_factory = GeneralDAGFactory("elt_pipeline_factory", dag_args=dag_args)
dag = dag_factory.get_airflow_dag(task_list=tasks)
