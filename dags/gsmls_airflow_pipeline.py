import sys
import os
import shelve
import pendulum
from airflow.sdk import task, dag, TaskGroup
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.exceptions import AirflowSkipException, AirflowFailException
from docker.types import Mount
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pendulum import timezone

# That guarantees the DAG loader process can find the plugins module even if
# Airflow ignores the PYTHONPATH
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))

# Define default args
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2026, 8, 18,
                           hour=9, minute=30, tzinfo=timezone("America/New_York")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }
description = """'
    GSMLS_Scrape_And_Preprocessing is a a pipeline which produced relational and image data for
    residential (RES), multifamily (MUL), land (LND), rental (RNT), and tax (TAX) data from the
    GSMLS. This pipeline uses Apache Kafka as an event-based message delivery system that produces data
    from .xls or .tsv into JSON data and reconverted to pandas. Thats data is then parsed to produce the image data
    into another Kafka topic, save the image metadata in MongoDB and clean/enrich the relational data
    before saving in PostgreSQL.
"""
prop_type_dict = {
        'RES': 'res_properties',
        'MUL': 'mul_properties',
        'TAX': 'tax_properties_new',
        'LND': 'lnd_properties',
        'RNT': 'rnt_properties',
    }

"""
---------------------------------------------------------------------------------------------------------------
                                    UTILITY FUNCTIONS COPIED FROM GSMLS.UTILITY_FUNC
                                    NECESSARY TO BYPASS AIRFLOW DEPENDENCY ISSUES
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


def create_volume_mounts(job: str):

    mount_list = []
    # The source_base for the Docker volume is relative to the host machine, not the environment the DpckerOperator
    # will be run in. The Docker daemon is mounted to Airflow, however, the Airflow daemon is rerouting tasks back
    # to the local host daemon
    source_base = '/root/home/projects/GSMLS-Analysis'
    container_base = '/app'
    jobs_dict = {
        'minor_job': {'source': ['pipeline_metadata', 'jobs', 'logs/logger_decorator'],
                      'target': ['pipeline_metadata', 'jobs', 'logs']},
        'producer': {'source': ['pipeline_metadata', 'data/stage_one/downloads', 'jobs', 'logs/logger_decorator'],
                     'target': ['pipeline_metadata', 'downloads', 'jobs', 'logs']},
        'consumer': {'source': ['pipeline_metadata', 'consumer_backup_data', 'jobs', 'logs/logger_decorator'],
                     'target': ['pipeline_metadata', 'consumer_backup_data', 'jobs', 'logs']},
        'image_consumer': {'source': ['pipeline_metadata', 'jobs', 'logs/logger_decorator'],
                           'target': ['pipeline_metadata', 'jobs', 'logs']},
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


def cutoff_time(
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    tz: str = None,
):

    start = pendulum.now(tz=timezone(tz))
    finish = start + timedelta(days=days)
    finish = finish.set(hour=hours, minute=minutes, second=seconds, microsecond=0)

    print(f" ==== THE CUTOFF TIME IS : {finish} ==== ")

    return finish


def get_filepath(usecase: str):

    # Filepaths needed inside the DockerOperator environments. The only host path needed is
    # for the environment file which is located on the host
    filepaths = {
        'env': ['/opt/airflow/.env '],
        'jobs_major': ['/app/jobs/major_jobs'],
        'jobs_minor': ['/app/jobs/minor_jobs'],
        'logger': ['/app/logs'],
        'pyspark_logs': ['/app/logs/pyspark_logs'],
        'metadata': ['/app/pipeline_metadata']
    }

    return filepaths[usecase][0]


"""
---------------------------------------------------------------------------------------------------------------
"""


def branching_decision(**kwargs):

    # starting_point returns True or False
    prop_type = kwargs['prop_type']
    status = kwargs['ti'].xcom_pull(task_ids=f'precheck_{prop_type.lower()}.get_current_status', key='return_value')
    print(f'Status: {status}')
    if status is True:

        # Returns the task_id based on the internal logic
        return cutoff_condition(prop_type.lower(), kwargs['pipeline_start'])

    else:
        return f"skip_all_tasks_{prop_type.lower()}"


def current_status(pipeline: str, prop_type: str, key=None):

    data_path = '/opt/airflow/pipeline_metadata'
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        if key is None:
            result = reader[pipeline][prop_type]
        else:
            result = reader[pipeline][prop_type][key]

    print(f'Pipeline: {pipeline} Key: {key}, Result: {result}')
    return result


def cutoff_condition(prop_type, pipeline_start: str):

    now = pendulum.now(tz=timezone("America/New_York"))
    pl_start = pendulum.parse(pipeline_start)
    end_of_day = pl_start.set(hour=23, minute=59, second=59)

    print(f' ==== PIPELINE START TIME: {pl_start} ==== ')
    print(f' ==== CURRENT TIME: {now} ==== ')
    print(f' ==== END OF DAY: {end_of_day} ==== ')

    if pl_start <= now <= end_of_day:
        start_cutoff = cutoff_time(days=1, hours=2, minutes=30, tz="America/New_York")
    else:
        start_cutoff = cutoff_time(hours=2, minutes=30, tz="America/New_York")

    end_cutoff = start_cutoff + timedelta(hours=1, minutes=30)

    if start_cutoff <= now <= end_cutoff:
        print(f' ==== PROGRAM CUTOFF CONDITION MET. SKIPPING {prop_type.upper()} DATA SCRAPE ==== ')
        return f'skip_all_tasks_{prop_type}'
    else:
        print(f' ==== PROGRAM CUTOFF CONDITION NOT MET. STARTING {prop_type.upper()} DATA SCRAPE ==== ')
        return f'start_pipeline_{prop_type}'


def image_processing_decision(**kwargs):

    prop_type = kwargs['prop_type']

    if prop_type in ['RES', 'MUL', 'RNT']:
        # Returns the task_id based on the internal logic
        return f"etl_pipeline_{prop_type.lower()}.image_processing"

    else:
        return f"etl_pipeline_{prop_type.lower()}.skip_image_tasks"


def new_msgs_available(topic, prop_type):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic}_msg_check", "data_consumer")
    producer_status = current_status('gsmls_airflow_pipeline', prop_type, 'producer')

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
        elif producer_status is not False:
            if isinstance(producer_status, int):
                return False
            else:
                # Triggers sensor to start consumers. Consumers will shut down in next phase
                print(' ==== GSMLS PRODUCER ENDED PRE-MATURELY. NO NEW DATA TO BE CONSUMED ==== ')
                return True

    except AttributeError:
        # Figure out how to properly handle or else this causes infinite loop of sensor not being trigger
        print(f"No partitions found for topic {topic}")

        return False


def skip_pipeline():
    raise AirflowSkipException("Time cutoff reached - skipping full pipeline")


@task(task_id="send_status_email")
def status_email(prop_type: str, phase: str = "Starting"):

    # https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
    results = current_status("gsmls_airflow_pipeline", prop_type)

    if phase is "Starting":
        postgres_results = results['postgresql_start']
        mongo_results = results['mongodb_start']
    else:
        postgres_results = results['postgresql_final']
        mongo_results = results['mongodb_final']

    postgres_table_name = postgres_results['table_name']
    mongo_table_name = mongo_results['table_name']
    mongo_status_ = mongo_results["mongo_status"]
    postgres_count = postgres_results["prop_count"]
    mongo_count = mongo_results["num_of_docs"]
    mongo_table_exists = mongo_results['mongo_col']
    rows_added = results['producer']

    if phase == "Starting":
        ip_address = os.getenv("DIGITAL_OCEAN_IP")
        subject = "GSMLS Pipeline Has Started"
        message = f"""
                    <br>
                    <b>Execution Date</b>: {{{{ ds }}}}<br>
                    <b>Pipeline Start Time</b>: {datetime.now()}<br>
                    <b>Kafka Connection Status</b>: Connected<br>
                    <b>MongoDB Collection Exists</b>: {mongo_table_exists}<br>
                    <b>MongoDB Connection Status</b>: {mongo_status_}<br>
                    <b>MongoDB Document Count</b>: {mongo_count}<br>
                    <b>MongoDB Table Name</b>: {mongo_table_name}<br>
                    <b>Postgres Row Count</b>: {postgres_count}<br>
                    <b>Postgres Table Name</b>: {postgres_table_name}<br>
                    <b>Property Type</b>: {prop_type}<br><br>

                    You can view the status and progress of your pipeline from the following ports:<br>
                    -- <b>Airflow</b>: http://{ip_address}:8085<br>
                    -- <b>Spark</b>: http://{ip_address}:8080<br>
                    -- <b>pgAdmin</b>: http://{ip_address}:5050 (pgAdmin)<br>
                    -- <b>Selenium</b>: http://{ip_address}:7900 <br>
                """
    else:

        # Make sure MongoDB Document Count gets changed to New Documents Added
        subject = "GSMLS Pipeline Has Finished"
        message = f"""
                    <br>
                    <b>Pipeline End Time</b>: {datetime.now()}<br>
                    <b>Kafka Connection Status</b>: Closed<br>
                    <b>MongoDB Connection Status</b>: Closed<br>
                    <b>MongoDB Document Count</b>: {mongo_count}<br>  
                    <b>Postgres Table Name</b>: {postgres_table_name}<br>
                    <b>Property Type</b>: {prop_type}<br>
                    <b>Postgres Rows Added</b>: {rows_added}<br><br>
                """

    send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)

    return True


# Define DAG as decorator over final pipeline function
@dag(
    "GSMLS_Scrape_And_Preprocessing",
    description=description,
    default_args=default_args,
    schedule=timedelta(days=1),
)
def gsmls_pipeline():

    previous_group = None
    # pipeline_start = str(pendulum.now(tz=timezone("America/New_York")))

    for prop_type, topic in prop_type_dict.items():

        with TaskGroup(group_id=f"precheck_{prop_type.lower()}") as pipeline_precheck:
            # Task 1: Check the health of Apache Kafka Connection
            kafka_conn = DockerOperator(
                task_id=f"check_kafka_connection",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/kafka_connection.py --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job')
            )

            # Task 2: Create and check the health of MongoDB Connection and if database exists
            mongo_start_results = DockerOperator(
                task_id=f"check_mongodb_connection",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/check_mongodb.py "
                        f"--db_name realEstate --table_name propertyImages --key mongodb_start --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )

            starting_point = DockerOperator(
                task_id=f"starting_point",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/starting_point.py --prop_type {prop_type} "
                        f"--table_name backfill_event_log",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )

            start_result = PythonOperator(
                task_id=f'get_current_status',
                python_callable=current_status,
                op_kwargs={'pipeline': 'gsmls_airflow_pipeline', 'key': 'start_point', 'prop_type': prop_type}
            )

            kafka_conn >> mongo_start_results >> starting_point >> start_result

        branch_decision = BranchPythonOperator(
            task_id=f'branching_decision_{prop_type.lower()}',
            python_callable=branching_decision,
            op_kwargs={'prop_type': prop_type, 'pipeline_start': "{{ data_interval_start.in_timezone('America/New_York') }}"},
            trigger_rule="none_failed"
        )

        skip_all_tasks = PythonOperator(
            task_id=f"skip_all_tasks_{prop_type.lower()}",
            python_callable=skip_pipeline,
            trigger_rule="all_success"
        )

        with TaskGroup(group_id=f"start_pipeline_{prop_type.lower()}") as start_pipeline:

            # Task 1a: Check if the correct topics have been created
            kafka_conn_status = PythonOperator(
                task_id=f'kafka_conn_status',
                python_callable=current_status,
                op_kwargs={'pipeline': 'gsmls_airflow_pipeline', 'key': 'kafka_connection', 'prop_type': prop_type}
            )

            kafka_topics = DockerOperator(
                task_id="check_kafka_topics",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/kafka_topics.py --topic {topic} "
                        f"--kafka_conn {{{{ ti.xcom_pull(task_ids='start_pipeline_{prop_type.lower()}.kafka_conn_status') }}}}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )

            # Task 4: Get row count of target table
            postgresql_start = DockerOperator(
                task_id="postgresql_start_data",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/get_postgresql_rows.py --table_name {topic} "
                        f"--key postgresql_start --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )

            # Task 5: Send pipeline initiation email
            email = status_email(prop_type=prop_type)

            kafka_conn_status >> kafka_topics >> postgresql_start >> email

        with TaskGroup(group_id=f"etl_pipeline_{prop_type.lower()}") as etl_pipeline:
            # Update so table_name and prop type isn't hard-coded

            # Task 6: Start the GSMLS message production
            data_producer = DockerOperator(
                task_id="gsmls_producer",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_major')}/gsmls_producer.py --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                user="1200:0",  # Matches the Selenium container user permissions
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('producer'),
                env_file=get_filepath("env")
            )

            # The producer will publish data to both the data and image topics first
            # Task decorators don't define __rshift__ ">>" so I need to use classic Operators for dependencies
            kafka_msg_sensor = PythonSensor(
                task_id="kafka_msg_sensor",
                python_callable=new_msgs_available,
                op_kwargs={'topic': topic, 'prop_type': prop_type},
                poke_interval=60,
                timeout=3600,
                mode="reschedule"
            )

            gsmls_consumer = DockerOperator(
                task_id="gsmls_data_consumer",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_major')}/gsmls_consumer.py --prop_type {prop_type} "
                        f"--topic {topic}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('consumer'),
                env_file=get_filepath("env")
            )

            image_decision = BranchPythonOperator(
                task_id=f'image_processing_decision',
                python_callable=image_processing_decision,
                op_kwargs={'prop_type': prop_type},
                trigger_rule="none_failed"
            )

            skip_image_tasks = PythonOperator(
                task_id=f"skip_image_tasks",
                python_callable=skip_pipeline,
                trigger_rule="all_success"
            )

            with TaskGroup(group_id=f"image_processing") as image_processing_pipeline:

                kafka_img_sensor = PythonSensor(
                    task_id=f"kafka_image_sensor",
                    python_callable=new_msgs_available,
                    op_kwargs={'topic': 'prop_images', 'prop_type': prop_type},
                    poke_interval=60,
                    timeout=3600,
                    mode="reschedule"
                )

                image_consumer = DockerOperator(
                    task_id="gsmls_image_consumer",
                    image="gsmls-jobs:0.9.6",
                    command=f"{get_filepath('jobs_major')}/gsmls_image_consumer.py --prop_type {prop_type} "
                            f"--order_num '79065846, 64872924'",
                    api_version="auto",
                    auto_remove='force',
                    mount_tmp_dir=False,
                    docker_url="unix://var/run/docker.sock",
                    network_mode="airflow_network",
                    mounts=create_volume_mounts('image_consumer'),
                    env_file=get_filepath("env")
                )

                kafka_img_sensor >> image_consumer

            merge = EmptyOperator(
                task_id=f"merge_tasks",
                trigger_rule="none_failed"
            )

            # ETL Pipeline dependencies
            kafka_msg_sensor >> gsmls_consumer
            branch_decision >> image_decision >> skip_image_tasks >> merge
            branch_decision >> image_decision >> image_processing_pipeline >> merge
            (data_producer, gsmls_consumer, image_processing_pipeline) >> merge

        with TaskGroup(group_id=f"ending_pipeline_{prop_type.lower()}") as ending_pipeline:

            mongo_final_results = DockerOperator(
                task_id="check_mongodb_connection2",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/check_mongodb.py "
                        f"--db_name realEstate --table_name propertyImages --key mongodb_final --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )
            postgresql_final = DockerOperator(
                task_id="postgresql_final_data",
                image="gsmls-jobs:0.9.6",
                command=f"{get_filepath('jobs_minor')}/get_postgresql_rows.py --table_name {topic} "
                        f"--key postgresql_final --prop_type {prop_type}",
                api_version="auto",
                auto_remove='force',
                mount_tmp_dir=False,
                docker_url="unix://var/run/docker.sock",
                network_mode="airflow_network",
                mounts=create_volume_mounts('minor_job'),
                env_file=get_filepath("env")
            )

            final_email = status_email(prop_type, phase='Ending')

            mongo_final_results >> postgresql_final >> final_email

        outside_merge = EmptyOperator(
            task_id=f"merge_tasks_{prop_type.lower()}",
            trigger_rule="none_failed"
        )

        # # Total pipeline dependencies
        pipeline_precheck >> branch_decision
        branch_decision >> skip_all_tasks >> outside_merge
        branch_decision >> start_pipeline >> etl_pipeline >> ending_pipeline >> outside_merge

        if previous_group:
            previous_group >> pipeline_precheck

        previous_group = outside_merge


# dag_instance = gsmls_pipeline()
gsmls_pipeline()

