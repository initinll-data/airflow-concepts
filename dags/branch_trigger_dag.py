from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
 
from datetime import datetime

@task(task_id='task_1')
def task_1():
    return 42
 
@task(task_id='task_2')
def task_2():
    None

@task(task_id='task_4', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def task_4():
    None

@task.branch(task_id="branching")
def branching(val):
    if val == 42:
        return 'task_2'
    return 'task_3'

@dag(dag_id='branch_trigger_dag', start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False) 
def branch_trigger_dag():
    t1 = task_1()

    branch = branching(t1)
 
    t2 = task_2()
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = task_4()
 
    t1 >> branch >> [t2, t3] >> t4

branch_trigger_dag()