from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 1, 1)
}

with DAG('parallel_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False) as dag:

    task1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )
    
    with TaskGroup(group_id='processing_tasks') as processing_tasks:
        task2 = BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
        )
        with TaskGroup(group_id="spark_tasks") as spark_task:
            task3 = BashOperator(
                task_id='task_3',
                bash_command='sleep 3'
            )
        with TaskGroup(group_id="flink_tasks") as flink_tasks: 
            task4 = BashOperator(
                task_id='task_4',
                bash_command='sleep 3'
            )
    
        task4 = BashOperator(
            task_id='task_4',
            bash_command='sleep 3'
        )

        task2 >> task4
        [spark_task, flink_tasks] >> task4 
    
    task5 = BashOperator(
        task_id='task_5',
        bash_command='sleep 3'
    )


    task1 >> processing_tasks >> task5