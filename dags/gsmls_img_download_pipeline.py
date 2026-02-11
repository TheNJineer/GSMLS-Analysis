import pendulum
import os
from datetime import timedelta
from datetime import datetime
from docker.types import Mount
from pendulum import timezone
from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator


# Use pendulum to restrict the cleaning to specific times of the day, when images aren't being downloaded
# or when gsmls pipeline isn't being run
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 2, 10,
                           hour=4, minute=45, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    GSMLS_Image_Downloading is a pipeline dedicated to querying GSMLS documents from
    a MongoDB database, extracting the Images field and associated metadata, downloading the
    images from the host server, and storing the images in an AWS S3 database. This pipeline
    will be initiated after the MongoDB_Database_Cleaning finishes and the
    GSMLS_Scrape_And_Preprocessing begins for the day. Image data will be used to train
    CNNs and object detection algorithms
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
                     'target': ['pipeline_metadata', 'parquet_files', 'logs']},
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


def cutoff_time(
    days: int = 0,
    hours: int = None,
    minutes: int = None,
    seconds: int = None,
    tz: str = None,
):

    start = pendulum.now(tz=timezone(tz))
    day = start.day
    day_delta = day + days
    finish = start.set(
        day=day_delta, hour=hours, minute=minutes, second=seconds, microsecond=0
    )

    assert finish > start, f" ==== CUTOFF TIME IS LESS THAN THE CURRENT DATETIME ==== "
    print(f" ==== THE CUTOFF TIME IS : {finish} ==== ")

    return finish


def get_filepath(usecase: str):

    filepaths = {
        'jobs_major': ['/workspace/jobs/major_jobs', '/app/major_jobs']
    }

    for path in filepaths[usecase]:
        if os.path.exists(path):
            return path

    raise ValueError(f" ==== CURRENT FILEPATHS FOR {usecase} DO NOT EXIST IN THIS ENVIRONMENT ==== ")


"""
---------------------------------------------------------------------------------------------------------------
"""


def cutoff_decision(tz: timezone):
    start = cutoff_time(hours=4, minutes=40, tz="America/New_York")
    end = cutoff_time(hours=7, tz="America/New_York")

    if start <= pendulum.now(tz) < end:
        return True
    else:
        return False


@dag(
    "GSMLS_Image_Downloading",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=1),
)
def download_images():

    decision = ShortCircuitOperator(
        task_id='cutoff_criteria',
        python_callable=cutoff_decision(timezone("America/New_York"))
    )

    downloads = DockerOperator(
        task_id="data_cleaning",
        image="gsmls-jobs:latest",
        command=f"{get_filepath('jobs_major')}/download_images.py --local",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('cleaning'),
        env_file='/root/home/projects/GSMLS-Analysis/.env'
    )

    decision >> downloads


# DAG Initiation
download_images()

