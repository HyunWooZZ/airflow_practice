from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

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
    
    task2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 3'
    )

    task3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 3'
    )

    task4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )


    task1 >> [task2, task3] >> task4