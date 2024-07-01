from abc import ABC, abstractmethod
from importlib import import_module
from datetime import datetime, timedelta
import logging

import pendulum

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.models.param import Param

logger = logging.getLogger('dag.factory')

MAPPED_OPERATORS = {
    "Python": {
        "path": "airflow.operators.python",
        "class": "PythonOperator",
    },
    "Empty": {
        "path": "airflow.operators.empty",
        "class": "EmptyOperator"
    },
    "Dummy": { # Deprecated, replaced by Empty
        "path": "airflow.operators.empty",
        "class": "EmptyOperator"
    },
    "TaskGroup": {
        "path": "airflow.utils.task_group",
        "class": "TaskGroup"
    }
}


def success_callback():
    log_success = logging.getLogger('airflow.task.callback')
    log_success.info("Task successful! Here we can send an email or slack message.")


def failure_callback():
    log_success = logging.getLogger('airflow.task.callback')
    log_success.info("Task failed! Here we can send an email or slack message.")


class DAGFactory(ABC):
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
    
    @property
    def dag_name(self) -> str:
        return self._dag_name
    
    @dag_name.setter
    def dag_name(self, value:str) -> None:
        self._dag_name = value

    @property
    def dag_args(self) -> dict:
        return self._dag_args
    
    @dag_args.setter
    def dag_args(self, value:str) -> None:
        self._dag_args = value


    @abstractmethod
    def add_tasks(self, dag:DAG, task_list:list[dict]) -> DAG:
        """
        Adds tasks to your DAG. The task dictionary contains all information necessary to create
        an instance of the desired Operators.

        :param DAG dag: An Airflow DAG object.
        :param list task_list: a list of task dictionaries. The format of these dictionaries should
            be defined by each Factory.

        :rtype: DAG
        """
    
    def get_airflow_dag(self, task_list:list[dict]) -> DAG:
        """
        Creates DAG using the args from the constructor and the tasks passed on the task list.

        :param list task_list: a list of dictionaries with your `operator` key, its parameters and
            a `dependencies` key with a list with the task upstream dependencies.

        :rtype: DAG
        """
        dag = DAG(self.dag_name, **self.dag_args)
        dag = self.add_tasks(dag, task_list)
        return dag

class PythonDAGFactory(DAGFactory):
    """
    Factory that creates a DAG that only use the Python operator.
    This simple factory creates the DAG with the default parameters.
    """

    def __init__(self, dag_name:str, dag_args:dict = {}) -> None:
        """
        Initialize all default parameters for a DAG.

        :param str dag_name: the DAG unique name to be shown on Airflow.
        :param dict dag_args: a dictionary with all DAG arguments, like catchup or schedule_interval.

        :rtype: None
        """
        super().__init__(dag_name, dag_args)
        operator_path = MAPPED_OPERATORS.get('Python')
        self.operators['Python'] = getattr(import_module(operator_path['path']), operator_path['class'])

    def add_tasks(self, dag:DAG, task_list:list[dict]) -> DAG:
        """
        Adds tasks to your DAG. The task dictionary contains all information necessary to create
        an instance of the desired Operator.

        :param DAG dag: An Airflow DAG object.
        :param list task_list: a list of task dictionaries. Should contain the Operator parameters
            and a list of dependencies of this task.

        :rtype: DAG
        """
        parents = []
        dependencies = {}
        tasks = {}

        # Create all tasks and creates a list of all parents of tasks
        for task in task_list:
            dependencies[task['task_id']] = task.pop('dependencies')
            # Associate task to DAG
            task['dag'] = dag
            tasks[task['task_id']] = self.operators['Python'](**task)
            for parent in dependencies[task['task_id']]:
                parents.append(parent)

        # Set up upstream dependencies
        for task in tasks.keys():
            if dependencies[task]:
                for dep in dependencies[task]:
                    tasks[dep] >> tasks[task]
        return dag

class GeneralDAGFactory(DAGFactory):
    """
    Factory that creates a DAG object using default values defined by the team's practices.
    This is a general factory that just adds the default parameters and creates the DAG as requested.
    """

    @DAGFactory.dag_args.setter
    def dag_args(self, value:str) -> None:
        if value.get("params"):
            value = self.add_dag_params(value)
        self._dag_args = value
    
    @staticmethod
    def add_default_task_parameters(task:dict) -> dict:
        """
        Adds the default parameters to a task dictionary for simple DAGs.
        Specifically, we add a failure callback, a timeout of 12 hours and the logger name.

        :param dict task: a task dictionary. It contains their `operator` key,
            its parameters and a `dependencies` key with a list of the task upstream dependencies.

        :rtype: dict
        """
        if not task.get('on_failure_callback'):
            task['on_failure_callback'] = failure_callback
        if not task.get('on_failure_callback'):
            task['execution_timeout'] = timedelta(hours=12)
        task['logger_name'] = 'airflow.simple_dag_factory.task'
        return task

    @staticmethod
    def get_operator(operator:str) -> BaseOperator:
        """
        Dynamically imports a mapped Operator so tasks can be created.
        
        :param str dag_name: key for an operator in the supported operators.
        
        :rtype: BaseOperator
        """
        operator_path = MAPPED_OPERATORS.get(operator)
        if not operator_path:
            raise ValueError(f"Operator {operator} is not supported!")
        return getattr(import_module(operator_path['path']), operator_path['class'])
    
    def add_dag_params(self, dag_args:dict) -> dict:
        """
        Adds Param to your DAG. You input a `dag_args` dictionary from where the Params will be popped.
        a Param object will be created for each Param and added to the DAG. 

        :param dict dag_args: a dictionary with all DAG arguments, like catchup or schedule_interval.

        :rtype: dict
        """
        params = dag_args.pop("params", {})
        dag_params = {}
        for param in params.keys():
            dag_params[param] = Param(**params[param])
        dag_args["params"] = dag_params
        return dag_args

    def add_tasks(self, dag:DAG, task_list:list[dict]) -> DAG:
        """
        Adds tasks to your DAG. The task dictionary contains all information necessary to create
        an instance of the desired Operators.

        :param DAG dag: An Airflow DAG object.
        :param list task_list: a list of task dictionaries. They contain an `operator` key,
            its parameters and a `dependencies` key with a list of the task upstream dependencies.

        :rtype: DAG
        """
        parents = []
        dependencies = {}
        tasks = {}
        
        # Create default begin and end tasks
        if task_list:
            if 'Empty' not in self.operators.keys():
                self.operators['Empty'] = self.get_operator('Empty')
            begin = self.operators['Empty'](
                task_id='begin',
                on_success_callback=success_callback,
                dag=dag
            )
            end = self.operators['Empty'](
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
            # Associate task to DAG
            task['dag'] = dag
            task = self.add_default_task_parameters(task)
            tasks[task['task_id']] = self.operators[operator](**task)
            for parent in dependencies[task['task_id']]:
                parents.append(parent)

        # Set up upstream dependencies
        for task in tasks.keys():
            if not dependencies[task] and not task['task_group']:
                begin >> tasks[task]
            else:
                for dep in dependencies[task]:
                    tasks[dep] >> tasks[task]
            if task not in parents and not task['task_group']:
                tasks[task] >> end
        return dag
