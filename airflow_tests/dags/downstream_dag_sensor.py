from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


from datetime import datetime


default_args = {
    'owner': 'atskhay',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3)
}

with DAG(
    dag_id="downstream_dag_sensor",
    default_args=default_args,
    schedule='@daily'
) as dag:
    sensor_A = ExternalTaskSensor(
        task_id="sensor_A",
        external_dag_id="upstream_dag_sensor",
        external_task_id="pyspark_task_1",
    )
    pyspark_task_2 = SparkSubmitOperator(
        task_id='pyspark_task_2',
        conn_id='sparksub',  # Имя вашего подключения к Spark
        application='/opt/from_host/02_sparkjob.py',  # Путь к вашему Spark приложению
        name='pyspark_task_2'
    )
    sensor_A >> pyspark_task_2 
