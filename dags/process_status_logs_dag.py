import os
import shelve
import json
import pendulum
from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pendulum import timezone


default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 9, 25,
                           hour=9, minute=30, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    Simple log processing pipeline which parses the GSMLS property statuses and saves the data in MongoDB
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


def create_kafka_consumer(client_id, group_id):

    return KafkaConsumer(
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=["broker-1:9092", "broker-2:9092", "broker-3:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8"),
        value_deserializer=lambda v: v.decode("utf-8"),
        heartbeat_interval_ms=5000,  # Send heartbeats in 5s intervals
        session_timeout_ms=45000,  # How long the consumer waits for heartbeats before considered dead: 45 secondds
        max_poll_interval_ms=3000000,  # How long the consumer goes in between successful polls before considered "stuck": 50 minutes
        max_poll_records=100,  # Max number of records pulled per poll request
    )


def new_msgs_available(topic, prop_type):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic}_msg_check", "data_consumer")

    # Check the partitions in the consumer. Returns a set of partition ids
    partitions = cons.partitions_for_topic(topic)
    print(f'{partitions}')

    try:
        if not partitions:
            print(f"No partitions found for topic {topic}")

            raise AttributeError(f"No partitions created for {topic}")

        # Create list of TopicPartition objects to check end offsets
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        offset_dict.update({f"{tp}": False for tp in topic_partitions})

        # Returns a dict of partitions and their end offsets in key-value pairs
        end_offsets = cons.end_offsets(topic_partitions)

        for tp in topic_partitions:
            # tp is the topic partition object
            committed = cons.committed(tp)
            latest = end_offsets[tp]

            if committed is None:
                committed = 0
            lag = latest - committed
            print(
                f"Partition {tp.partition}: committed={committed}, latest={latest}, lag={lag}"
            )

            if lag > 0:
                offset_dict[tp] = True

        if True in list(offset_dict.values()):
            print(f"New data found for {topic}")
            return True
        else:
            return False

    except AttributeError:
        # Figure out how to properly handle or else this causes infinite loop of sensor not being trigger
        print(f"No partitions found for topic {topic}")

        return False


@dag(
    "Property_Status_Parsing",
    description=description,
    default_args=default_args,
    schedule=timedelta(hours=1),
)
def parse_log_statuses():

    progress = DockerOperator(
        task_id="parse_log_status",
        image="gsmls-jobs:0.8.9",  # Update this image
        command=f"{get_filepath('jobs_major')}/process_property_status_logs.py",
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

    progress >> merge


# DAG Initiation
parse_log_statuses()

