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
                           hour=3, minute=45, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    MongoDB_Database_Cleaning is a pipeline that periodically cleans the MongoDB
    database(s) of malformed and duplicate documents. Due to restarts and programmatic
    error, these instances can occur frequently. This pipeline will run daily as data
    will be produced daily. Due to the probability of creating corrupted data, this pipeline
    will only run after a data scrape has occurred and before the GSMLS_Image_Downloading
    pipeline is initiated.
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
        'jobs_major': ['/workspace/jobs/major_jobs', '/app/major_jobs'],
    }

    for path in filepaths[usecase]:
        if os.path.exists(path):
            return path

    raise ValueError(f" ==== CURRENT FILEPATHS FOR {usecase} DO NOT EXIST IN THIS ENVIRONMENT ==== ")


"""
---------------------------------------------------------------------------------------------------------------
"""


def cutoff_decision(tz: timezone):

    start = cutoff_time(hours=3, minutes=45, tz="America/New_York")
    end = cutoff_time(hours=4, minutes=30, tz="America/New_York")

    if start <= pendulum.now(tz) < end:
        return True
    else:
        return False


@dag(
    "MongoDB_Database_Cleaning",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=1),
)
def database_cleaning():

    decision = ShortCircuitOperator(
        task_id='data_ready',
        python_callable=cutoff_decision(timezone("America/New_York")),
    )

    cleaning = DockerOperator(
        task_id="data_cleaning",
        image="gsmls-jobs:latest",
        command=f"{get_filepath('jobs_major')}/clean_mongodb_data.py --local",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('cleaning'),
        env_file='/root/home/projects/GSMLS-Analysis/.env'
    )

    decision >> cleaning


# DAG Initiation
database_cleaning()

