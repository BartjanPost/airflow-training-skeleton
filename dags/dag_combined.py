import datetime as dt

from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from HTTP_Operator import HttpToGcsOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator)
from godatadriven.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


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


dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="gdd-32ba4f8b4a2ca57e5b201b0062",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
    auto_delete_ttl=5 * 60,  # Autodelete after 5 minutes
)


df_to_bg = DataFlowPythonOperator(
    task_id="Dataflow_to_BigQuery",
    dataflow_default_options={
        "project": "gdd-32ba4f8b4a2ca57e5b201b0062",
        "region": "europe-west1",
    },
    py_file="gs://airflow_training_bp/dataflow_job.py",
    dag=dag,
)


for currency in {'EUR', 'USD'}:
    currency_task = HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="airflow_training_http",
        gcs_conn_id="airflow_training_gcs_bucket",
        gcs_bucket="airflow_training_bp",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
    currency_task >> dataproc_create_cluster
    currency_task >> df_to_bg


compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://airflow_training_bp/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)


dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id="gdd-32ba4f8b4a2ca57e5b201b0062",
)


gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="gcs_to_BigQuery",
    bucket="airflow_training_bp",
    source_objects=["average_prices/transfer_date={{ ds }}/*"],
    destination_project_dataset_table="gdd-32ba4f8b4a2ca57e5b201b0062:prices.land_registry_price${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)


pgsl_to_gcs >> dataproc_create_cluster
dataproc_create_cluster >> compute_aggregates
compute_aggregates >> dataproc_delete_cluster
compute_aggregates >> gcs_to_bq
pgsl_to_gcs >> df_to_bg
