import os
import shelve
import json
import pendulum
from airflow.sdk import task, dag
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from pendulum import timezone


default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 3, 17,
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
        'minor_job': {'source': ['pipeline_metadata', 'jobs', 'logs/logger_decorator'],
                      'target': ['pipeline_metadata', 'jobs', 'logs']}
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
        'env': ['/opt/airflow/.env '],
        'jobs_minor': ['/app/jobs/minor_jobs'],
        'metadata': ['/app/pipeline_metadata']
    }

    return filepaths[usecase][0]


"""
---------------------------------------------------------------------------------------------------------------
"""


def current_status(**kwargs):

    status = {
        'producer': None,
        'data_consumer': None,
        'image_consumer': None
    }

    value = kwargs['ti'].xcom_pull(task_ids='get_metadata', key='metadata')
    result = json.loads(value)

    for key in status.keys():
        status[key] = result[key]

    if False in list(status.values()):
        # Condition not met, send email
        print(' ==== DATA IS STILL BEING PRODUCTION/CONSUMED ==== ')
        return True
    else:
        # Condition met, skip email
        print(' ==== PRODUCTION AND CONSUMPTION HAVE BEEN COMPLETED ==== ')
        return False


def data_cleaning_progress():
    pass


def get_metadata(**kwargs):

    data_path = '/opt/airflow/pipeline_metadata'
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        metadata = reader["gsmls_airflow_pipeline"]

    prop_type, result = get_latest_prop_type(metadata)

    # Convert int64 datatypes to int to clear TypeError during JSON conversion
    for key in ['mongodb_start', 'mongodb_final', 'postgresql_start', 'postgresql_final', 'progress_tracker']:
        if key in ['mongodb_start', 'mongodb_final']:
            if result[key] is not None:
                result[key]['num_of_docs'] = int(result[key]['num_of_docs'])
        elif key in ['postgresql_start', 'postgresql_final']:
            if result[key] is not None:
                result[key]['prop_count'] = int(result[key]['prop_count'])
        elif key == 'progress_tracker':
            for sub_key in ['year', 'county', 'rows_produced', 'documents', 'split_index']:
                if result[key] is None:
                    pass
                elif result[key][sub_key] is not None:
                    result[key][sub_key] = int(result[key][sub_key])

    print(result)

    kwargs['ti'].xcom_push(key='metadata', value=json.dumps(result))
    kwargs['ti'].xcom_push(key='prop_type', value=prop_type)


def image_download_progress():
    pass


def get_latest_prop_type(results):

    prop_type = None
    best_time = None

    for key in results.keys():
        if prop_type is None:
            prop_type = key
            if results[key]['timestamp'] is not None:
                best_time = pendulum.parse(results[key]['timestamp'])
        else:
            if results[key]['timestamp'] is not None:
                if best_time < pendulum.parse(results[key]['timestamp']):
                    prop_type = key
                    best_time = pendulum.parse(results[key]['timestamp'])

    return prop_type, results[prop_type]


def progress_update(**kwargs):

    subject = "GSMLS Pipeline Update"
    value = kwargs['ti'].xcom_pull(task_ids='get_metadata', key='metadata')
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
        python_callable=get_metadata
    )

    progress = DockerOperator(
        task_id="pipeline_progress",
        image="gsmls-jobs:0.8.9",
        command=f"{get_filepath('jobs_minor')}/progress_update.py "
                f"--prop_type {{{{ ti.xcom_pull(task_ids='get_metadata', key='prop_type') }}}} "
                f"--pipeline gsmls_airflow_pipeline",
        api_version="auto",
        auto_remove='force',
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mounts=create_volume_mounts('minor_job'),
        env_file=get_filepath("env")
    )

    status = ShortCircuitOperator(
        task_id='check_status',
        python_callable=current_status
    )

    progress_email = PythonOperator(
        task_id='progress_email',
        python_callable=progress_update
    )

    merge = EmptyOperator(
        task_id=f"merge_tasks",
        trigger_rule="none_failed"
    )

    property_type >> progress >> status >> progress_email >> merge
    status >> progress_email >> merge
    status >> merge


# DAG Initiation
gsmls_email()





