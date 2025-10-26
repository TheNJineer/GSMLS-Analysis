import time
import json
import sys
import kafka.errors
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from airflow.sdk import task, dag, TaskGroup
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.sensors.python import PythonSensor
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.admin.client import KafkaAdminClient
from pymongo.errors import ConnectionFailure
from plugins.GSMLS import GSMLS
from plugins.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from plugins.utility_func import logger_decorator, create_kafka_consumer
from plugins.utility_func import create_sql_engine, create_kafka_producer, create_mongodb_conn

# That guarantees the DAG loader process can find the plugins module even if
# Airflow ignores the PYTHONPATH
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))


def airflow_data_consumer(prop_type, retry):

    consumer = KafkaGSMLSConsumer()
    consumer.main(prop_type, retry)


def airflow_image_consumer(prop_type='IMAGES', retry=False):

    img_consumer = KafkaGSMLSConsumer()
    img_consumer.main(prop_type, retry)


def condition_function(value):

    if value == 'complete':
        return False
    else:
        return True


def starting_point():

    prop_type_dict = {
        'RES': 'res_properties',
        'MUL': 'mul_properties',
        'LND': 'lnd_properties',
        'RNT': 'rnt_properties',
        'TAX': 'tax_properties'
    }

    query = """
            SELECT * FROM gsmls_event_log_new
            ORDER BY id DESC
            LIMIT 1;
            """

    engine = create_sql_engine("gsmls", remote=True)
    metadata = pd.read_sql_query(query, con=engine.raw_connection())
    last_row = metadata.shape[0] - 1

    if metadata.empty:

        for prop_type, topic in prop_type_dict.items():
            yield prop_type, topic

    else:
        start_date = metadata.loc[last_row, "start_date"]
        last_scraped_county = metadata.loc[last_row, "county"]
        last_scraped_muni = metadata.loc[last_row, "municipality"]
        finished = metadata.loc[last_row, "finished"]
        last_scraped_property_type = metadata.loc[last_row, "property_type"]
        index_num = list(prop_type_dict.keys()).index(last_scraped_property_type)
        modified_prop_list = list(prop_type_dict.keys())[index_num:]
        modified_topic_list = list(prop_type_dict.values())[index_num:]

        for prop_type, topic in zip(modified_prop_list, modified_topic_list):
            # All data for the last property type was acquired
            if (last_scraped_county == 30 and last_scraped_muni == "White Twp." and finished == "Yes"
                    and last_scraped_property_type != 'TAX'):
                try:
                    if start_date < datetime.now() - timedelta(days=1):
                        yield prop_type, topic
                    else:
                        if last_scraped_property_type == 'TAX':
                            yield 'complete', 'complete'
                        else:
                            # Return the next property in the list
                            continue
                except TypeError:
                    yield prop_type, topic

            elif (last_scraped_county == 30 and last_scraped_muni == "White Twp." and finished == "Yes"
                    and last_scraped_property_type == 'TAX'):
                return 'complete', 'complete'

            else:
                return prop_type, topic


def new_msgs_available(topic, logger_):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic} msg_check", f"{topic} msg_check")

    # Check the partitions in the consumer. Returns a set of partition ids
    partitions = cons.partitions_for_topic(topic)

    if not partitions:
        logger_.warning(f"No partitions found for topic {topic}")

        raise AttributeError(f"No partitions created for {topic}")

    # Create list of TopicPartition objects to check end offsets
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    offset_dict.update({f"{tp}": False for tp in topic_partitions})

    end_offsets = cons.end_offsets(
        topic_partitions
    )  # Returns a dict of partitions and their end offsets

    for tp in topic_partitions:
        # tp is the topic partition object
        committed = cons.committed(tp)
        latest = end_offsets[tp]

        if committed is None:
            committed = 0
        lag = latest - committed
        logger_.info(
            f"Partition {tp.partition}: committed={committed}, latest={latest}, lag={lag}"
        )

        if lag > 0:
            offset_dict[tp] = True

    if True in list(offset_dict.values()):
        return True
    else:
        return False


@task(task_id="gsmls_producer")
def airflow_gsmls_producer(prop_type, **kwargs):

    obj = GSMLS(prop_type)
    kwargs['property_type'] = prop_type
    obj.airflow_gsmls_producer(**kwargs)


# Task 1: Check the health of Apache Kafka #Connection
@task(task_id="check_kafka_connection")
def check_kafka_connection(logger_):

    # Check if broker connects
    test_producer = create_kafka_producer(client_id='test-connection')

    if test_producer.bootstrap_connected() is True:
        logger_.info('Test connection to Kafka brokers was successful')
        test_producer.close()
        return True

    else:
        return False

    # Make sure these brokers are created in the #Docker Compose yaml
    # brokers_ready = {4: False, 5: False, 6: False}
    #
    # admin_client = KafkaClient(
    #     bootstrap_servers=["broker-1:9092", "broker-2:9092", "broker-3:9092"],
    #     client_id="health_check",
    # )
    # admin_client.poll(timeout_ms=1000)
    #
    # # Step 1: Individual broker checks
    # while list(brokers_ready.values()).count(True) < 2:
    #
    #     for id_ in brokers_ready.keys():
    #         conn_result = admin_client.is_ready(node_id=id_)
    #         brokers_ready[id_] = conn_result
    #
    #     if not list(brokers_ready.values()).count(True) >= 2:
    #         # Need this to be able to check if more than one node isn’t connected
    #         unconnected_node = list(brokers_ready.values()).index(False)
    #         logger_.info(
    #             f"Broker {unconnected_node} is not ready. Retrying connection"
    #         )
    #
    # else:
    #     admin_client.close()
    #     return True


# Task 1a: Check if the correct topics have been created
@task(task_id="create_topics")
def create_kafka_topics(logger_, topic: str = 'res_properties', status: bool = True):

    topic_list = ["prop_images", topic]

    if status is True:

        admin_client = KafkaAdminClient(
            bootstrap_servers=["broker-1:9092", "broker-2:9092", "broker-3:9092"],
            client_id="check_topic",
        )

        available_topics = admin_client.list_topics()
        logger_.info(f'Current existing topics: {available_topics}')

        for t in topic_list:

            try:
                if t not in available_topics:
                    topic_obj = NewTopic(
                        name=t,
                        num_partitions=3,
                        replication_factor=2,
                        topic_configs={"cleanup.policy": "compact"},  # Look into what other configs I need
                    )
                    admin_client.create_topics(
                        new_topics=[topic_obj], validate_only=False, timeout_ms=1500
                    )
            except kafka.errors.TopicAlreadyExistsError:
                logger_.info(f'Topic {t} already exists')
            else:
                logger_.info(f'Topic {t} created in Apache Kafka topic list')

        return admin_client.list_topics()


# Task 2: Create and check the health of MongoDB Connection and if database exists
@task(task_id="check_mongo_connection", multiple_outputs=True)
def check_mongodb(db_name, table_name, logger_):

    retries = 0

    client = create_mongodb_conn(remote=True)

    # Confirm connection to MongoDB Atlas
    while retries < 10:

        try:
            client.admin.command({"ping": 1})
        except ConnectionFailure as cf:
            logger_.warning(f"{cf}")
            time.sleep(1)
            retries += 1
        else:
            logger_.info('MongoDB connection successful')
            break

    else:
        raise ConnectionFailure(f"Table {table_name} does not exist")

    # Collect database information
    database = client[db_name]
    table_result = table_name in database.list_collection_names()
    num_of_docs_ = database[table_name].count_documents({})

    if table_result is True:
        return {'mongo_status': 'Connected', 'table_name': table_name,
                'mongo_col': table_result, 'num_of_docs': num_of_docs_}
    else:
        return {'mongo_status': 'Connected', 'table_name': table_name,
                'mongo_col': table_result, 'num_of_docs': num_of_docs_}


# Task 4: Get row count of res_properties table
@task(task_id="postgres_data_count", multiple_outputs=True)
def get_postgresql_rows(table_name_, remote=True):

    engine = create_sql_engine("gsmls", remote=remote)

    query = f"SELECT COUNT(*) FROM {table_name_};"

    # The version discrepancy between Pandas 2.x and SQLAlchemy 1.4.x forces
    # the user to create a raw DBAPI connection which Pandas expects
    # Throws AttributeError "Engine/Connection object has no .cursor() method"
    df = pd.read_sql_query(query, con=engine.raw_connection())

    return {'table_name': table_name_, 'prop_count': int(df.loc[0].values[0])}


# Task 5: Send pipeline initiation email
@task(task_id="send_status_email")
def status_email(postgres_results, mongo_results, phase: str = "Starting"):

    # https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
    postgres_table_name = postgres_results['table_name']
    mongo_table_name = mongo_results['table_name']
    mongo_status_ = mongo_results["mongo_status"]
    postgres_count = postgres_results["prop_count"]
    mongo_count = mongo_results["num_of_docs"]
    mongo_table_exists = mongo_results['mongo_col']

    property_types = {
        "res_properties": "RES",
        "mul_properties": "MUL",
        "lnd_properties": "LND",
        "rnt_properties": "RNT",
        "tax_properties": "TAX",

    }

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
                    <b>Property Type</b>: {property_types[postgres_table_name]}<br><br>

                    You can view the status and progress of your pipeline from the following ports:<br>
                    -- <b>Airflow UI</b>: http://{ip_address}:8085<br>
                    -- <b>Spark UI</b>: http://{ip_address}:8080<br>
                    -- <b>MongoDB UI</b>: http://{ip_address}:8081 (Mongo Express)<br>
                    -- <b>PostgreSQL UI</b>: http://{ip_address}:5050 (pgAdmin)<br>
                    -- <b>Selenium UI</b>: http://{ip_address}:7900 (Install VNC viewer for OS to view browser)<br>
                """
    else:

        subject = "GSMLS Pipeline Has Finished"
        message = f"""
                    <br>
                    <b>Pipeline End Time</b>: {datetime.now()}<br>
                    <b>Kafka Connection Status</b>: Closed<br>
                    <b>MongoDB Connection Status</b>: Closed<br>
                    <b>MongoDB Document Count</b>: {mongo_count}<br>
                    <b>Postgres Table Name</b>: {postgres_table_name}<br>
                    <b>Property Type</b>: {property_types[postgres_table_name]}<br>
                    <b>Postgres Row Count</b>: {postgres_count}<br><br>
                """

    send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)


# Define default args
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['jqhameed@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2025, 10, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Define DAG as decorator over final pipeline function
@dag(
    "GSMLS_Pipeline",
    description="",
    default_args=default_args,
    schedule=timedelta(days=7),
)
@logger_decorator
def gsmls_pipeline(**kwargs):

    load_dotenv()
    logger = kwargs["logger"]
    previous_group = None

    for prop_type, topic in starting_point():

        short_circuit = ShortCircuitOperator(
            task_id=f'short_circuit_{prop_type.lower()}',
            python_callable=condition_function,
            op_kwargs={'value': prop_type}
        )

        with TaskGroup(group_id=f"start_pipeline_{prop_type.lower()}") as start_pipeline:

            # Check the Kafka connection
            kafka_conn = check_kafka_connection(logger)
            # Create Kafka topics if necessary
            _ = create_kafka_topics(logger, topic=topic, status=kafka_conn)
            mongo_start_results = check_mongodb("realEstate-cloud", "propertyImages", logger)
            postgresql_results1 = get_postgresql_rows(topic)
            status_email(postgresql_results1, mongo_start_results)

        with TaskGroup(group_id=f"etl_pipeline_{prop_type.lower()}") as etl_pipeline:
            # Update so table_name and prop type isn't hard-coded
            # Task 5: Start the GSMLS message production
            airflow_gsmls_producer(prop_type)
            # The producer will publish data to both the data and image topics first
            # Task decorators don't define __rshift__ ">>" so I need to use classic Operators for dependencies
            kafka_msg_sensor = PythonSensor(
                task_id="kafka_msg_sensor",
                python_callable=new_msgs_available,
                op_kwargs={'topic': topic, 'logger_': logger},
                poke_interval=300,
                timeout=3600,
                mode="reschedule"
            )

            kafka_img_sensor = PythonSensor(
                task_id=f"kafka_image_sensor_{prop_type.lower()}",
                python_callable=new_msgs_available,
                op_kwargs={'topic': 'prop_images', 'logger_': logger},
                poke_interval=300,
                timeout=3600,
                mode="reschedule"
            )

            # Task 6: Start the GSMLS consumer and MongoDB consumer
            gsmls_consumer = PythonOperator(
                task_id=f"gsmls_consumer_{prop_type.lower()}",
                python_callable=airflow_data_consumer,
                op_kwargs={"prop_type": prop_type, "retry": False})
    
            image_consumer = PythonOperator(
                task_id=f"image_consumer_{prop_type.lower()}",
                python_callable=airflow_image_consumer,
                op_kwargs={"retry": False})

            # ETL Pipeline dependencies
            kafka_msg_sensor >> gsmls_consumer
            kafka_img_sensor >> image_consumer

        with TaskGroup(group_id=f"ending_pipeline_{prop_type.lower()}") as ending_pipeline:

            mongo_end_results = check_mongodb("realEstate", "propertyImages", logger)
            postgresql_results2 = get_postgresql_rows(topic)
            status_email(postgresql_results2, mongo_end_results, phase='Ending')
        #
        # Total pipeline dependencies
        # short_circuit >> start_pipeline >> etl_pipeline >> ending_pipeline
        short_circuit >> start_pipeline >> ending_pipeline

        if previous_group:
            previous_group >> short_circuit

        previous_group = ending_pipeline


gsmls_pipeline()
