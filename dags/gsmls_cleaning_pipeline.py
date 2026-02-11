import json
import os
import shelve
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from pendulum import timezone
from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


# Define default args
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 2, 10,
                           hour=3, minute=15, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    GSMLS_Staged_Cleaning is a pipeline that prepares data from the GSMLS_Scrape_And_Preprocessing
    pipeline and further cleans and prepares it for use in training Deep Neural Networks.
    Stage 1: Correcting municipal and county codes and correcting datatypes
    Stage 2: Use PySpark to load municipal tax data and merge specific columns with the target data
    Stage 3: Data enrichment and final cleaning
"""


"""
---------------------------------------------------------------------------------------------------------------
                                    UTILITY FUNCTIONS COPIED FROM GSMLS.UTILITY_FUNC
                                    NECESSARY TO BYPASS AIRFLOW DEPENDENCY ISSUES
---------------------------------------------------------------------------------------------------------------
"""


def create_volume_mounts(job: str):

    mount_list = []
    source_base = '/root/home/projects/GSMLS-Analysis'
    container_base = '/app'
    jobs_dict = {
        'cleaning': {'source': ['pipeline_metadata', 'data/stage_one/parquet_files', 'logs/pyspark_logs'],
                     'target': ['pipeline_metadata', 'parquet_files', 'logs']}
    }

    source_list = jobs_dict[job]['source']
    target_list = jobs_dict[job]['target']

    for source, target in zip(source_list, target_list):
        mount_obj = Mount(
            source=os.path.join(source_base, source),
            target=os.path.join(container_base, target),
            type='bind'
        )
        mount_list.append(mount_obj)

    return mount_list


def get_filepath(usecase: str):

    filepaths = {
        'jobs_major': ['/workspace/jobs/major_jobs', '/app/major_jobs'],
    }

    for path in filepaths[usecase]:
        if os.path.exists(path):
            return path

    raise ValueError(f" ==== CURRENT FILEPATHS FOR {usecase} DO NOT EXIST IN THIS ENVIRONMENT ==== ")


"""
---------------------------------------------------------------------------------------------------------------
"""


def data_sensor(**context):
    # Short circuit the cleaning if the gsmls_airflow_pipeline is currently running or
    # if the prop_type isn't RES
    pulled_value = context['ti'].xcom_pull(task_ids='get_pipeline_status', key='pipeline_status')
    value = json.loads(pulled_value)
    if value['prop_type'] != 'RES':
        return False
    elif value['prop_type'] == 'RES' and isinstance(value['pipeline_status'], bool):
        return False
    else:
        return True


def get_pipeline_status(**context):

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        result = reader["gsmls_airflow_pipeline"]
        prop_type = result["prop_type"]
        pipeline_status = result["producer"]

    value = json.dumps({'prop_type': prop_type, 'pipeline_status': pipeline_status})
    context['ti'].xcom_push(key='pipeline_status', value=value)


@dag(
    "GSMLS_Staged_Cleaning",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=7),
)
def gsmls_cleaning_pipeline():

    status = PythonOperator(
        task_id='get_pipeline_status',
        python_callable=get_pipeline_status,
        provide_context=True
    )

    data_ready = ShortCircuitOperator(
        task_id='data_ready',
        python_callable=data_sensor,
        provide_context=True
    )

    # This job needs to be created
    # clean_duplicates = DockerOperator(
    #     task_id="data_cleaning",
    #     image="gsmls-jobs:latest",
    #     command=f"{get_filepath('jobs_major')}/remove_duplicate_data.py",
    #     api_version="auto",
    #     auto_remove=True,
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="airflow_network",
    #     mount=create_volume_mounts('cleaning'),
    #     env_file='/root/home/projects/GSMLS-Analysis/.env'
    # )

    data_cleaning = DockerOperator(
        task_id="data_cleaning",
        image="gsmls-jobs:latest",
        command=f"{get_filepath('jobs_major')}/phased_cleaning.py",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('cleaning'),
        env_file='/root/home/projects/GSMLS-Analysis/.env'
    )

    status >> data_ready >> data_cleaning


# DAG Initiation
gsmls_cleaning_pipeline()

