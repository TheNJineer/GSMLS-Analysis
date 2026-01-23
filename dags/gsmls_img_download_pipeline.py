import pendulum
from datetime import timedelta
from datetime import datetime
from pendulum import timezone
from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from gsmls_core.gsmls.utility_func import cutoff_time, get_filepath, create_volume_mounts


# Use pendulum to restrict the cleaning to specific times of the day, when images aren't being downloaded
# or when gsmls pipeline isn't being run
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 1, 13,
                           hour=7, minute=5, tzinfo=timezone("America/New_York")),
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


def cutoff_decision(tz: timezone):
    start = cutoff_time(hours=7, tz="America/New_York")
    end = cutoff_time(hours=9, tz="America/New_York")

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
        command=f"{get_filepath('jobs_major')}/download_images.py",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('cleaning')
    )

    decision >> downloads