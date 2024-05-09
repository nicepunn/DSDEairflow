import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/assets/jars/postgresql-42.2.6.jar"

all_file = "/usr/local/spark/assets/data/output_scraping/scrap.csv"
postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="data-proj-dag",
    description="DAG for scrapping ans save to postgres",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

data_scraping = SparkSubmitOperator(
    task_id="scraping",
    application="/usr/local/spark/applications/scraping-spark.py",
    name="scrapping",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/applications/load-postgres.py",
    name="load-postgres",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[all_file ,
                      postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

spark_job_read_postgres = SparkSubmitOperator(
    task_id="spark_job_read_postgres",
    application="/usr/local/spark/applications/read-postgres.py",
    name="read-postgres",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> data_scraping >> spark_job_load_postgres >> spark_job_read_postgres >> end