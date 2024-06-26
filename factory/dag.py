from importlib import import_module
from datetime import datetime
import logging

import pendulum

from airflow import DAG
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger('dag.factory')

SUPPORTED_OPERATORS = {
    "python": {
        "path": "airflow.operators.python",
        "class": "PythonOperator",
    },
    "empty": {
        "path": "airflow.operators.empty",
        "class": "EmptyOperator"
    },
    "dummy": { # Deprecated, replaced by Empty
        "path": "airflow.operators.empty",
        "class": "EmptyOperator"
    }
}


def success_callback():
    log_success = logging.getLogger('airflow.task.callback')
    log_success.info("Task successful! Here we can send an email or slack message.")


def failure_callback():
    log_success = logging.getLogger('airflow.task.callback')
    log_success.info("Task failed! Here we can send an email or slack message.")


class DAGFactory:
    """
    Factory that creates a DAG object using default values defined by the team's practices.
    We define here the basic structure for a DAG Factory that can be extended by its subclasses.
    """

    def __init__(self, dag_name:str, dag_args:dict = {}) -> None:
        """
        Initialize all default parameters for a DAG.

        :param str dag_name: the DAG unique name to be shown on Airflow.
        :param dict dag_args: a dictionary with all DAG arguments, like catchup or schedule_interval.

        :rtype: None
        """
        self.dag_name = dag_name
        self.dag_args = dag_args
        self.operators = {}
        default_args = {
            'owner': 'DAG Dev Team',
            'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone('America/Sao_Paulo'))
        }
        if dag_args.get('default_args'):
            default_args.update(dag_args.get('default_args'))
        self.dag_args['default_args'] = default_args
        self.dag_args['catchup'] = dag_args.get('catchup', False)
        self.dag_args['tags'] = dag_args.get('tags', []) + ['Factory']
        self.dag_args['render_template_as_native_obj'] = dag_args.get('render_template_as_native_obj', True)

    def get_operator(operator:str) -> BaseOperator:
        """
        Dynamically imports a supported Operator so tasks can be created.
        
        :param str dag_name: key for an operator in the supported operators.
        
        :rtype: BaseOperator
        """
        operator_path = SUPPORTED_OPERATORS.get(operator)
        if not operator_path:
            raise ValueError(f"Operator {operator} is not supported!")
        return getattr(import_module(operator_path['path']), operator_path['class'])
    
    def add_default_task_parameters():
        pass

    def add_tasks(self, dag:DAG, task_list:list[dict]) -> DAG:
        """
        Adds tasks to your DAG. The task dictionary contains task dependencies and Operator parameters.

        :param DAG dag: An Airflow DAG object.
        :param list task_list: a list of dictionaries with your `operator` key, its parameters and
            a `dependencies` key with a list with the task upstream dependencies.

        :rtype: DAG
        """
        parents = []
        dependencies = {}
        tasks = {}
        
        # Create default begin and end tasks
        if task_list:
            if 'empty' not in self.operators.keys():
                self.operators['empty'] = self.get_operator('empty')
            begin = self.operators['empty'](
                task_id='begin',
                on_success_callback=success_callback,
                dag=dag
            )
            end = self.operators['empty'](
                task_id='end',
                on_success_callback=success_callback,
                trigger_rule='none_failed',
                dag=dag
            )

        # Create all tasks and creates a list of all parents of tasks
        for task in task_list:
            dependencies[task['task_id']] = task.pop('dependencies')
            # Import operator if needed
            operator = task.pop('operator')
            if operator not in self.operators.keys():
                self.operators[operator] = self.get_operator(operator)
            # Creates task in the given DAG
            task['dag'] = dag
            tasks[task['task_id']] = self.operators[operator](**task)
            for parent in dependencies[task['task_id']]:
                parents.append(parent)

        # Set up upstream dependencies
        for task in tasks.keys():
            if not dependencies[task]:
                begin >> tasks[task]
            else:
                for dep in dependencies[task]:
                    tasks[dep] >> tasks[task]
            if task not in parents:
                tasks[task] >> end
        return dag

    def get_airflow_dag(self, task_list:list[dict]):
        """
        Creates DAG using the args from the constructor and the tasks passed on the task list.

        :param list task_list: a list of dictionaries with your `operator` key, its parameters and
            a `dependencies` key with a list with the task upstream dependencies.

        :rtype: DAG
        """
        dag = DAG(self.dag_name, **self.dag_args)
        dag = self.add_tasks(dag, task_list)
        return dag
