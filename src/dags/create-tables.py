from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import os
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
    dag_id="create-tables",
    description='A DAG to create tables and set foreign key constraint',
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

create_article_table = PostgresOperator(
    task_id='create_article_table',
    postgres_conn_id='conn_postgres',  # Connection ID configured in Airflow Connections
    sql='''
    CREATE TABLE "Article" (
      "id" SERIAL PRIMARY KEY,
      "title" VARCHAR,
      "abstract" VARCHAR,
      "publishdate" DATE
    );
    ''',
    dag=dag,
)

create_article_keywords_table = PostgresOperator(
    task_id='create_article_keywords_table',
    postgres_conn_id='conn_postgres',  # Connection ID configured in Airflow Connections
    sql='''
    CREATE TABLE "ArticleKeywords" (
      "id" SERIAL PRIMARY KEY,
      "article_id" INT,
      "keyword" VARCHAR
    );
    ''',
    dag=dag,
)

add_foreign_key_constraint = PostgresOperator(
    task_id='add_foreign_key_constraint',
    postgres_conn_id='conn_postgres',  # Connection ID configured in Airflow Connections
    sql='''
    ALTER TABLE "ArticleKeywords"
    ADD CONSTRAINT fk_article_id FOREIGN KEY ("article_id") REFERENCES "Article" ("id");
    ''',
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> create_article_table >> create_article_keywords_table >> add_foreign_key_constraint >> end
