import os
import pendulum
from airflow.sdk import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from pendulum import timezone


default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 9, 28,
                           hour=23, minute=00, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    Simple image manifest pipeline which parses the AWS image manifest and saves the tabulated data in PostgreSQL
"""


"""
---------------------------------------------------------------------------------------------------------------
                                    UTILITY FUNCTIONS COPIED FROM GSMLS.UTILITY_FUNC
                                    NECESSARY TO BYPASS AIRFLOW DEPENDENCY ISSUES
---------------------------------------------------------------------------------------------------------------
"""


def create_manifest_date():

    day_of_week = int(datetime.now().strftime("%w"))

    if day_of_week == 0:
        return datetime.now().strftime("%Y-%m-%d")
    else:
        _manifest_date = datetime.now() - timedelta(days=day_of_week)
        return _manifest_date.strftime("%Y-%m-%d")


def create_volume_mounts(job: str):

    mount_list = []
    source_base = '/root/home/projects/GSMLS-Analysis'
    container_base = '/app'
    jobs_dict = {
        'major_job': {'source': ['jobs', 'logs/logger_decorator'],
                      'target': ['jobs', 'logs']}
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
        'jobs_major': ['/app/jobs/major_jobs'],
        'metadata': ['/app/pipeline_metadata']
    }

    return filepaths[usecase][0]


"""
---------------------------------------------------------------------------------------------------------------
"""


@dag(
    "AWS_Image_Manifest",
    description=description,
    default_args=default_args,
    schedule=timedelta(hours=1),
)
def update_image_manifest():

    manifest_date = PythonOperator(
        task_id=f'get_manifest_date',
        python_callable=create_manifest_date,
    )

    progress = DockerOperator(
        task_id="update_image_manifest",
        image="gsmls-jobs:0.9.7",  # Update this image
        command=f"{get_filepath('jobs_major')}/update_image_manifest.py "
                f"--date_str {{ ti.xcom_pull(task_ids='get_manifest_date') }}T01-00Z, "
                f"'--save_type', 'manifest'",
        api_version="auto",
        auto_remove='force',
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mounts=create_volume_mounts('major_job'),
        env_file=get_filepath("env")
    )

    merge = EmptyOperator(
        task_id=f"merge_tasks",
        trigger_rule="none_failed"
    )

    manifest_date >> progress >> merge


# DAG Initiation
update_image_manifest()

