import os
import shelve
import json
from airflow.sdk import task, dag
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from pendulum import timezone


default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 1, 14,
                           hour=9, minute=30, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    GSMLS_Emailing is a pipeline which reads metadata from the shelve file and reports on the
    status of different jobs in the GSMLS_Scrape_And_Preprocessing pipeline
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
        'minor_job': {'source': ['pipeline_metadata'],
                      'target': ['pipeline_metadata']}
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
        'metadata': ['/root/home/projects/GSMLS-Analysis/pipeline_metadata',
                     '/workspace/pipeline_metadata', '/app/pipeline_metadata']
    }

    for path in filepaths[usecase]:
        if os.path.exists(path):
            return path

    raise ValueError(f" ==== CURRENT FILEPATHS FOR {usecase} DO NOT EXIST IN THIS ENVIRONMENT ==== ")


"""
---------------------------------------------------------------------------------------------------------------
"""


def current_status(**context):

    status = {
        'producer': None,
        'data_consumer': None,
        'image_consumer': None
    }

    value = context['ti'].xcom_pull(task_ids='get_metadata', key='metadata')
    result = json.loads(value)

    for key in status.keys():
        status[key] = result[key]

    if False in list(status.values()):
        # Condition not met, send email
        return True
    else:
        # Condition met, skip email
        return False


def data_cleaning_progress():
    pass


def get_metadata(**context):

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        result = reader["gsmls_airflow_pipeline"]

    context['ti'].xcom_push(key='metadata', value=json.dumps(result))
    context['ti'].xcom_push(key='prop_type', value=result['prop_type'])


def image_download_progress():
    pass


def progress_update(**context):

    subject = "GSMLS Pipeline Update"
    value = context['ti'].xcom_pull(task_ids='get_metadata', key='metadata')
    result = json.loads(value)
    message = result["progress_message"]

    send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)


@dag(
    "GSMLS_Emailing",
    description=description,
    default_args=default_args,
    schedule=timedelta(hours=1),
)
def gsmls_email():

    property_type = PythonOperator(
        task_id='get_metadata',
        python_callable=get_metadata,
        provide_context=True
    )

    progress = DockerOperator(
        task_id="pipeline_progress",
        image="gsmls-jobs:latest",
        command=f"{get_filepath('jobs_minor')}/progress_update.py "
                f"--prop_type {{ ti.xcom_pull(task_ids='get_metadata', key='prop_type') }} "
                f"--pipeline gsmls_airflow_pipeline",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('minor_job'),
        env_file='/root/home/projects/GSMLS-Analysis/.env'
    )

    status = ShortCircuitOperator(
        task_id='check_status',
        python_callable=current_status,
        provide_context=True
    )

    status_email = PythonOperator(
        task_id='send_email',
        python_callable=progress_update,
        provide_context=True
    )

    property_type >> progress
    status >> status_email


# DAG Initiation
gsmls_email()





