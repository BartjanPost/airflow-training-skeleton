import datetime as dt

from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from HTTP_Operator import HttpToGcsOperator


dag = DAG(
    dag_id="my_first_combined_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 6, 20),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    dag=dag,
    sql="select * from land_registry_price_paid_uk where transfer_date='{{ds}}'",
    bucket="airflow_training_bp",
    filename="land_registry_price_paid_uk/{{ds}}/properties_{}.json",
    postgres_conn_id="airflow_training_postgres"
)


for currency in {'EUR', 'USD'}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="airflow_training_http",
        gcs_conn_id="airflow_training_gcs_bucket",
        gcs_bucket="airflow_training_bp",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
