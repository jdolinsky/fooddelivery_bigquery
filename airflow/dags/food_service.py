from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
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
LOCATION = "us-central1"
BUCKET = "food-orders-us"
NAME_PREFIX = "food_daily"

LOCAL_PATH = "/tmp/food"

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
        # destination folder for cleaned files
        "dest": "processed/", 
        # cleaned file folder
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
         start_date=pendulum.datetime(2024, 2, 21, tz=time_zone),
         default_args=default_args,
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
    upload_file_to_gcs = LocalFilesystemToGCSOperator(task_id="pcrocessed_file_to_gcs",
                                                      src="{{params.local_path}}/{{task_instance.xcom_pull('list_files', key='return_value')}}",
                                                      dst="{{params.dest}}{{task_instance.xcom_pull('list_files', key='return_value')}}")
    
    # delete downloaded data file
    #delete_original_file = BashOperator(task_id="delete_downloaded_file", bash_command="rm {{}}") 
    
    data_file_available >> list_files >> download_file >> [delete_file_from_gcs, clean_data] >> upload_file_to_gcs