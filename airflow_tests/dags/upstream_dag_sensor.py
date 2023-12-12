from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


default_args = {
    'owner': 'atskhay',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3)
}

with DAG(
    dag_id="upstream_dag_sensor",
    default_args=default_args,
    schedule='@daily'
) as dag:
  get_kerberos_ticket = BashOperator(
    task_id="kerberos_authentification",
    bash_command="kinit -Vfpkt /opt/from_host/airblast.keytab airblast@HADOOP.BDPAK.FRONTIER.KZ",
  )
  pyspark_task_1 = SparkSubmitOperator(
    task_id='pyspark_task_1',
    conn_id='sparksub',  # Имя вашего подключения к Spark
    application='/opt/from_host/01_sparkjob.py',  # Путь к вашему Spark приложению
    name='pyspark_task_1'
    )
  get_kerberos_ticket >> pyspark_task_1 
