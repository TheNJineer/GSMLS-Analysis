import time
import pandas as pd
import os
import pendulum
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from datetime import time as dtime
from airflow.sdk import task, dag, TaskGroup
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.exceptions import AirflowSkipException, AirflowFailException
from pendulum import timezone
from plugins.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from plugins.utility_func import logger_decorator, create_sql_engine


def data_sensor():
    pass

def stage_one_cleaning():
    pass


def stage_two_cleaning():
    pass


def stage_three_cleaning():
    pass


# Define default args
eastern = timezone("America/New_York")
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2025, 12, 17, hour=9, minute=30, tzinfo=eastern),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }


@dag(
    "GSMLS_Staged_Cleaning",
    description="",
    default_args=default_args,
    schedule=timedelta(days=1),
)
@logger_decorator
def gsmls_cleaning_pipeline(**kwargs):

    logger = kwargs["logger"]
    f_handler = kwargs["f_handler"]
    c_handler = kwargs["c_handler"]

    data_ready = PythonSensor(
        task_id='data_ready',
        python_callable=data_sensor,
        timeout=3600,
        mode='reschedule'
    )

    stage_one = PythonOperator(
        task_id='stage_one_cleaning',
        python_callable=stage_one_cleaning,
        op_kwargs={'logger': logger}
    )

    stage_two = PythonOperator(
        task_id='stage_two_cleaning',
        python_callable=stage_two_cleaning,
        op_kwargs={'logger': logger}
    )

    stage_three = PythonOperator(
        task_id='stage_three_cleaning',
        python_callable=stage_three_cleaning,
        op_kwargs={'logger': logger}
    )

    data_ready >> stage_one >> stage_two >> stage_three

