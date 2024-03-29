from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

import pendulum
import pandas as pd
import logging
import scripts.cleanup as cl
import os


#set timezone for pendulum
time_zone = 'America/Chicago'

# GCS Stuff
BUCKET = "food-orders-us"
NAME_PREFIX = "food_daily"

default_args = {
    "owner": "JD",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    # google cloud operator/sesor parameters
    "gcp_conn_id": "google_cloud_food_service",
    "google_cloud_conn_id": "google_cloud_food_service",
    "prefix": "food_daily",
    "bucket": "food-orders-us",
    "bucket_name": "food-orders-us",
    # other parameters
    "params": {
        "local_path":"/tmp/food",
        # destination folder for cleaned files in GCS
        "dest": "processed/", 
        # cleaned file folder on local
        "clean_dest": "clean"
    }
}


# executables
def list_file_names(bucket, prefix):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_food_service")
    files = gcs_hook.list(bucket_name=bucket,
                          prefix=prefix)
    if files :
        return files[0]
    else : 
        return None


# [START instantiate_dag]
with DAG(dag_id="food_service_pipeline",
         schedule_interval="@daily",
         start_date=pendulum.datetime(2024, 3, 21, tz=time_zone),
         default_args=default_args,
         max_active_runs=1,
         catchup=False
         ) as dag:
# [END instantiate_dag]
        
    data_file_available = GCSObjectsWithPrefixExistenceSensor(task_id="wait_for_file",
                                                   mode='poke',
                                                   poke_interval = 60,
                                                   # stop after 4 minutes
                                                   timeout=240 
                                                   )

    list_files = PythonOperator(task_id="list_files",
                                python_callable=list_file_names,
                                # pass it into executable
                                op_kwargs={"bucket": "food-orders-us", "prefix": "food_daily"},
                                do_xcom_push=True
                                )
    
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name="{{task_instance.xcom_pull('list_files', key='return_value')}}",
        filename=os.path.join("{{params.local_path}}", "{{task_instance.xcom_pull('list_files', key='return_value')}}"),
    )

    # clean data in downloaded csv
    clean_data = PythonOperator(task_id="clean_data",
                                python_callable=cl.data_cleanup,
                                op_kwargs={"file_path": "{{params.local_path}}", 
                                           "file_name": "{{task_instance.xcom_pull('list_files', key='return_value')}}",
                                           "file_dest": "{{params.clean_dest}}"}
                                )
    
    # delete file from google storage once it was downloaded
    delete_file_from_gcs = GCSDeleteObjectsOperator(task_id="delete_from_GCS",
                                                    prefix=None,
                                                    objects=["{{task_instance.xcom_pull('list_files', key='return_value')}}"]
                                                    )
    
    # upload cleaned file to gcs /processed folder
    upload_file_to_gcs = LocalFilesystemToGCSOperator(task_id="clean_file_to_gcs",
                                                      src="{{params.local_path}}/{{params.clean_dest}}/{{task_instance.xcom_pull('list_files', key='return_value')}}",
                                                      dst="{{params.dest}}{{task_instance.xcom_pull('list_files', key='return_value')}}")
    
    # delete downloaded data file
    delete_original_file = BashOperator(task_id="delete_downloaded_file", 
                                        bash_command="rm {{params.local_path}}/{{task_instance.xcom_pull('list_files', key='return_value')}}") 
    
    # delete cleaned data file and clean folder from local
    delete_clean_folder = BashOperator(task_id="delete_clean_folder", 
                                       bash_command="rm -rf {{params.local_path}}/{{params.clean_dest}}")

    # Transfer data to BigQuery 
    transfer_to_bq = GCSToBigQueryOperator(task_id="transfer_from_gs_to_bq",
                                           destination_project_dataset_table="dataset_food_orders.delivery_orders",
                                           source_objects="{{params.dest}}{{task_instance.xcom_pull('list_files', key='return_value')}}", 
                                           write_disposition='WRITE_APPEND',
                                           create_disposition='CREATE_IF_NEEDED',
                                           autodetect=True,
                                           # schema location on GS
                                           schema_object="schema/schema.json",
                                           skip_leading_rows=0)
    
    # Delete processed file from gs 
    delete_processed_file_from_gcs = GCSDeleteObjectsOperator(task_id="delete_processed_from_GCS",
                                                    prefix=None,
                                                    objects=["{{params.dest}}{{task_instance.xcom_pull('list_files', key='return_value')}}"]
                                                    )

    data_file_available >> list_files >> download_file >> [delete_file_from_gcs, clean_data] 
    clean_data >> [delete_original_file, upload_file_to_gcs] 
    upload_file_to_gcs >> [delete_clean_folder, transfer_to_bq] >> delete_processed_file_from_gcs 