import json
import os
import pendulum
import shelve
from datetime import datetime
from datetime import timedelta
from docker.types import Mount
from pendulum import timezone
from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Define default args
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 9, 27,
                           hour=3, minute=15, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """
    GSMLS_Staged_Cleaning is a pipeline that prepares data from the GSMLS_Scrape_And_Preprocessing
    pipeline and further cleans and prepares it for use in training Deep Neural Networks.
    Stage 1: Correcting municipal and county codes and correcting datatypes
    Stage 2: Use PySpark to load municipal tax data and merge specific columns with the target data
    Stage 3: Data enrichment and final cleaning
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
        'cleaning': {'source': ['pipeline_metadata', 'data/stage_one/parquet_files', 'logs/pyspark_logs', 'jobs'],
                     'target': ['pipeline_metadata', 'parquet_files', 'logs', 'jobs']}
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
        'jobs_major': ['/app/jobs/major_jobs'],
        'metadata': ['/app/pipeline_metadata']
    }

    return filepaths[usecase][0]


"""
---------------------------------------------------------------------------------------------------------------
"""


def data_sensor(**kwargs):
    # Short circuit the cleaning if the gsmls_airflow_pipeline is currently running or
    # if the prop_type isn't RES
    pulled_value = kwargs['ti'].xcom_pull(task_ids='get_pipeline_status', key='pipeline_status')
    value = json.loads(pulled_value)

    if isinstance(value['pipeline_status'], bool):
        return False
    elif isinstance(value['pipeline_status'], int):
        return True


# def get_latest_prop_type(results):
#
#     prop_type = None
#     best_time = None
#
#     for key in results.keys():
#         if prop_type is None:
#             prop_type = key
#             if results[key]['timestamp'] is not None:
#                 best_time = pendulum.parse(results[key]['timestamp'])
#         else:
#             if results[key]['timestamp'] is not None:
#                 if best_time < pendulum.parse(results[key]['timestamp']):
#                     prop_type = key
#                     best_time = pendulum.parse(results[key]['timestamp'])
#
#     return prop_type, results[prop_type]


def get_pipeline_status(**kwargs):

    data_path = '/opt/airflow/pipeline_metadata'
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        metadata = reader["gsmls_airflow_pipeline"]
        results = metadata['RES']
        producer_results = results['producer']
        if not isinstance(producer_results, bool):
            producer_results = int(producer_results)

    value = json.dumps({'pipeline_status': producer_results,
                        'data_consumer': results['data_consumer'],
                        'image_consumer': results['image_consumer']})
    kwargs['ti'].xcom_push(key='pipeline_status', value=value)


@dag(
    "GSMLS_Staged_Cleaning",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=7),
)
def gsmls_cleaning_pipeline():

    status = PythonOperator(
        task_id='get_pipeline_status',
        python_callable=get_pipeline_status
    )

    data_ready = ShortCircuitOperator(
        task_id='data_ready',
        python_callable=data_sensor
    )

    # This job needs to be created
    # clean_duplicates = DockerOperator(
    #     task_id="data_cleaning",
    #     image="gsmls-jobs:0.9.6",
    #     command=f"{get_filepath('jobs_major')}/remove_duplicate_data.py",
    #     api_version="auto",
    #     auto_remove='force,
    #     mount_tmp_dir=False,
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="airflow_network",
    #     mounts=create_volume_mounts('cleaning'),
    #     env_file=get_filepath('env')
    # )

    data_cleaning = DockerOperator(
        task_id="data_cleaning",
        image="gsmls-jobs:0.9.6",
        # entrypoint="bash",
        command=f"{get_filepath('jobs_major')}/phased_cleaning.py --table_name res_properties",
        # command="-c 'ls -R /app'",
        api_version="auto",
        auto_remove='force',
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_network",
        mounts=create_volume_mounts('cleaning'),
        env_file=get_filepath('env')
    )

    merge = EmptyOperator(
        task_id=f"merge_tasks",
        trigger_rule="none_failed"
    )

    status >> data_ready >> data_cleaning >> merge
    data_ready >> data_cleaning >> merge
    data_ready >> merge


# DAG Initiation
gsmls_cleaning_pipeline()

