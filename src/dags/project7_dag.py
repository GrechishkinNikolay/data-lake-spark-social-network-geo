from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'kolaygrech',
    'start_date': datetime(2025, 10, 29),
}

dag = DAG(
    dag_id='project7',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['spark', 'project7', 'etl'],
)

CODE_PATH = '/lessons/dags'
SPARK_CONF = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "1",
    "spark.executor.instances": "12",
    "spark.executor.memoryOverhead": "1024",
    "spark.dynamicAllocation.enabled": "false",
    "spark.sql.shuffle.partitions": "24"
}

user_activity = SparkSubmitOperator(
    task_id='user_activity_task',
    dag=dag,
    conn_id='yarn_spark',
    application=f'{CODE_PATH}/user_activity.py',
    application_args=[
        "10",  # streak_days
        # "27",  # streak_days
        "/user/kolaygrech/data/geo/events",  # events_base_path
        "/user/kolaygrech/data/analytics/user_activity/"  # output_base_path
    ],
    conf=SPARK_CONF
)

geo_zones = SparkSubmitOperator(
    task_id='geo_zones_task',
    dag=dag,
    conn_id='yarn_spark',
    application=f'{CODE_PATH}/geo_zones.py',
    application_args=[
        "/user/kolaygrech/data/geo/events",                 # events_base_path
        "/user/kolaygrech/data/analytics/geo_zones/"        # output_base_path
    ],
    conf=SPARK_CONF
)

friend_recommend = SparkSubmitOperator(
    task_id='friend_recommend_task',
    dag=dag,
    conn_id='yarn_spark',
    application=f'{CODE_PATH}/friend_recommend.py',
    conf=SPARK_CONF
)
user_activity >> geo_zones >> friend_recommend
