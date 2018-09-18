import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator


dag = DAG(
    dag_id="my_first_postgres_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 6, 20),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

"""
def print_exec_date(**context):
    print(context["execution_date"])
"""
#
#my_task = PythonOperator(
#    task_id="task_name",
#    python_callable=print_exec_date,
#    provide_context=True,
#    dag=dag
#)
#

pgsl_to_gcs= PostgresToGoogleCloudStorageOperator
(
   task_id="postgres_to_gcs",
   dag=dag,
   sql="select * from land_registry_price_paid_uk where trnasfer_date='{{ds}}'",
   bucket="airflow_training_bp",
   filename="land_registry_price_paid_uk/{{ds}}/properties_{}.json",
   postgre_conn_id="airflow_training_postgres"
   )