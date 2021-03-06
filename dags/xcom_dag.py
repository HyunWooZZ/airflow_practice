from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy', value=accuracy)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(key="model_accuracy", task_ids=[
        'processing_tasks.training_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])
    models = ["a", "b", "c"]
    accuracies = zip(models, accuracies)

    for model, accuracy in accuracies:
        if accuracy > 5:
            ti.xcom_push(key="best_model", value=model)
            return "accurate"
        else:
            return "inaccurate"


with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    downloading_data = BashOperator(
        do_xcom_push=False,
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    accurate = DummyOperator(
        task_id='accurate'
    )
    inaccurate = DummyOperator(
        task_id='inaccurate'
    )
    


    downloading_data >> processing_tasks >> choose_best_model

    choose_best_model >> [accurate, inaccurate]