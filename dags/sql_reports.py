import os
import json
import logging

from factory.dag import PythonDAGFactory

logger = logging.getLogger()

def get_report_config(path:str) -> list[dict]:
    """Gets the content of all json files from a path as dict"""
    json_files = [config for config in os.listdir(path) if config.endswith('.json')]
    report_configs = []
    for config_file in json_files:
        with open(os.path.join(path, config_file)) as f:
            config = json.load(f)
            config['name'] = config_file.replace(".json", "")
            report_configs.append(config)
    return report_configs

def generate_report(sql_file:str = "", source:str = "", email:str = "") -> None:
    """Generates and send a report based on a config file"""
    logger.info(f"Connecting to source: {source}")
    logger.info(f"Exporting result from {sql_file} to remote file system.")
    logger.info(f"Report sent to {email}!")

# Folder as found in the container
reports = get_report_config("/opt/airflow/dags/reports/")
dag_factory = PythonDAGFactory()

for report in reports:
    task = [{
        "task_id": f"run_{report['name']}",
        "python_callable": generate_report,
        "op_kwargs": {
            "sql_file": report["sql_file"],
            "source": report["source"],
            "email": report["email"]
        },
        "dependencies": []
    }]
    dag_factory.dag_name = report["name"]
    dag_factory.dag_args = {"schedule": report["schedule"]}
    globals()[report["name"]] = dag_factory.get_airflow_dag(task)
