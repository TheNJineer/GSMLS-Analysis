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
    "start_date": datetime(2026, 1, 14,
                           hour=4, minute=45, tzinfo=timezone("America/New_York")),
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


def cutoff_decision(tz: timezone):

    start = cutoff_time(hours=4, minutes=45, tz="America/New_York")
    end = cutoff_time(hours=7, tz="America/New_York")

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
        command=f"{get_filepath('jobs_major')}/clean_mongodb_data.py",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mount=create_volume_mounts('cleaning')
    )

    decision >> cleaning