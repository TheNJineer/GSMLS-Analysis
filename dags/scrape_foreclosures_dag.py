import pendulum
import os
import time
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
    "start_date": datetime(2026, 11, 16,
                           hour=5, minute=00, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    Scrape_Foreclosures is a pipeline that periodically scrapes the 
    Salesweb foreclosure listings.
"""


"""
---------------------------------------------------------------------------------------------------------------
                                    UTILITY FUNCTIONS COPIED FROM GSMLS.UTILITY_FUNC
                                    NECESSARY TO BYPASS AIRFLOW DEPENDENCY ISSUES
---------------------------------------------------------------------------------------------------------------
"""


def create_volume_mounts(job: str):

    mount_list = []
    source_base = '/root/home/projects/GSMLS-Analysis-Dev'
    container_base = '/app'
    jobs_dict = {
        'cleaning': {'source': ['pipeline_metadata', 'logs/logger_decorator', 'jobs'],
                     'target': ['pipeline_metadata', 'logs', 'jobs']}
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
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    tz: str = None,
    flag: str = 'start_time'
):

    start = pendulum.now(tz=timezone(tz)).set(hour=5, minute=28)  # Update this
    finish = start + timedelta(days=days)
    finish = finish.set(hour=hours, minute=minutes, second=seconds, microsecond=0)

    try:
        assert finish > start, f" ==== CUTOFF TIME IS LESS THAN THE CURRENT DATETIME ==== "
        print(f" ==== THE CUTOFF TIME IS : {finish} ==== ")
    except AssertionError as e:
        print(f'{e}')
        return False

    if flag == 'start_time':
        print(' ==== FORECLOSURE SCRAPING WILL BEGIN SOON ==== ')
        time.sleep(10)
        return finish
    else:
        return finish


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


def cutoff_decision(tz: str):

    # start = cutoff_time(hours=2, minutes=31, tz="America/New_York")
    end = cutoff_time(hours=6, minutes=30, tz="America/New_York", flag='end_time')  # Update this

    if isinstance(end, bool):
        return False
    elif pendulum.now(timezone(tz)) < end:
        return True
    else:
        return False


@dag(
    "Scraping_Foreclosures",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=7),
)
def scraping_foreclosures():

    scraping_decision = ShortCircuitOperator(
        task_id='cutoff_decision',
        python_callable=cutoff_decision,
        op_kwargs={'tz': "America/New_York"}
    )

    start_scraping = DockerOperator(
        task_id="data_scraping",
        image="gsmls-jobs:0.10.8",
        command=f"{get_filepath('jobs_major')}/salesweb_foreclosures.py",
        api_version="auto",
        auto_remove='force',
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        network_mode="airflow_network",
        mounts=create_volume_mounts('cleaning'),
        env_file=get_filepath('env')
    )

    scraping_decision >> start_scraping


# DAG Initiation
scraping_foreclosures()

