from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

import logging
import time
from datetime import datetime


default_args = {
    'owner': 'atskhay',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3)
}

# Инициализация DAG
dag = DAG('parallel_dag', 
          default_args=default_args, 
          schedule_interval=None
          )


# Авторизация в керберос
get_kerberos_ticket = BashOperator(
        task_id="kerberos_authentification",
        bash_command="kinit -Vfpkt /opt/from_host/airblast.keytab airblast@HADOOP.BDPAK.FRONTIER.KZ",
        dag=dag
)


# Простая питоновская функция
def hello_world_func():
        time.sleep(30)
        logging.info("Hello World! I've been waiting for 30 seconds to start!")

python_task_1 = PythonOperator(
    task_id='python_task_1',
    python_callable=hello_world_func,
    dag=dag
)

python_task_2 = PythonOperator(
    task_id='python_task_2',
    python_callable=hello_world_func,
    dag=dag
)


# Определение задачи SparkSubmitOperator
pyspark_task_1 = SparkSubmitOperator(
    task_id='pyspark_task_1',
    conn_id='sparksub',  # Имя вашего подключения к Spark
    application='/opt/from_host/01_sparkjob.py',  # Путь к вашему Spark приложению
    name='pyspark_task_1',
    verbose=False,
    dag=dag
)


pyspark_task_2 = SparkSubmitOperator(
    task_id='pyspark_task_2',
    conn_id='sparksub',  # Имя вашего подключения к Spark
    application='/opt/from_host/02_sparkjob.py',  # Путь к вашему Spark приложению
    name='pyspark_task_2',
    verbose=False,
    dag=dag
)


get_kerberos_ticket >> [python_task_1, python_task_2, pyspark_task_1, pyspark_task_2]