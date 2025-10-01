import time
import json
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from utility_func import get_us_pw, logger_decorator
from airflow.sdk import task, dag, PokeReturnValue, TaskGroup
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator
from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.admin.client import KafkaAdminClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from sqlalchemy import create_engine
from GSMLS import GSMLS
from Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from RealEstateImages import RealEstateImages
from kafka.errors import NoBrokersAvailable


# Define default args
default_args = {
    'owner': 'Jibreel Hameed',
    'start_date': datetime(2025, 8, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


def create_kafka_producer(logger_):
    retries = 0

    while retries <= 4:
        for attempt in range(1):
            try:
                producer = KafkaProducer(bootstrap_servers=['broker-1:9092', 'broker-2:9092', 'broker-3:9092'],
                                         key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                         retries=3, acks='all', client_id='data_producer')

                return producer

            except NoBrokersAvailable as nba:
                retries += 1
                logger_.warning(f'{nba}')
                logger_.warning(f"KafkaProducer couldn't be established on this attempt. This will be retry #{retries}")
                time.sleep(3)

                if retries > 4:
                    break

    raise NoBrokersAvailable


def create_kafka_consumer(client_id, group_id):

    return KafkaConsumer(client_id=client_id, group_id=group_id,
                         bootstrap_servers=['broker-1:9092', 'broker-2:9092', 'broker-3:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False, value_deserializer=lambda v: v.decode('utf-8'),
                         consumer_timeout_ms=120000)


def create_mongo_client():
    # Make sure the local MongoDB has been migrated to the cloud. Use cloud client info here

    return MongoClient('info_here')


# Define DAG as decorator over final pipeline function
@logger_decorator
@dag('GSMLS_Pipeline', description='', default_args=default_args, schedule=timedelta(days=7))
def gsmls_pipeline(**kwargs):

    # Task 1: Check the health of Apache Kafka #Connection
    @task(task_id='check_kafka_connection')
    def check_kafka_connection(logger_):

        # Make sure these brokers are created in the #Docker Compose yaml
        brokers_ready = {1: False, 2: False, 3: False}
        client_ready = False

        admin_client = KafkaClient(bootstrap_servers=['broker-1:9092', 'broker-2:9092', 'broker-3:9092'],
                                   client_id='health_check')
        admin_client.poll(timeout_ms=1000)

        # Step 1: Individual broker checks
        while (brokers_ready[1] | brokers_ready[2] | brokers_ready[3]) is False:

            for id_ in brokers_ready.keys():

                conn_result = admin_client.is_ready(node_id=id_)
                brokers_ready[id_] = conn_result

            if list(brokers_ready.values()).count(True) < 3:
                # Need this to be able to check if more than one node isn’t connected
                unconnected_node = list(brokers_ready.values()).index(False)
                logger_.info(f'Broker {unconnected_node} is not ready. Retrying connection')

        else:
            admin_client.close()
            return True

    # Task 1a: Check if the correct topics have been created
    @task(task_id='create_topics')
    def create_kafka_topics(topic: str, status: bool):

        if status is True:

            admin_client = KafkaAdminClient(bootstrap_servers=['broker-1:9092', 'broker-2:9092', 'broker-3:9092'],
                                            client_id='check_topic')
            topic_list = admin_client.list_topics()

            if topic not in topic_list:

                topic_obj = NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=2,
                    topic_configs={"cleanup.policy": "compact"} # Look into what other configs I need
                )

                admin_client.create_topics(new_topics=[topic_obj], validate_only=False)

    # Task 2: Check the health of MongoDB Connection and if database exists
    @task(task_id='check_mongo_connection')
    def check_mongodb(client, db_name, table_name, logger):

        retries = 0

        while retries < 10:

            try:
                conn_result = client.admin.command('ping')
            except ConnectionFailure as cf:
                logger.warning(f"{cf}")
                time.sleep(3)
                retries += 1
            else:

                try:
                    database = client[db_name]
                    table_result = table_name in database.list_collection_names()
                    num_of_docs_ = database[table_name].count_documents({})

                    if conn_result is True and table_result is True:
                        return True, database[table_name], num_of_docs_

                except ConnectionFailure as cf:
                    logger.warning(f"{cf}")
                    raise ConnectionFailure(f'Table {table_name} does not exist')

        else:
            raise ConnectionFailure(f'Table {table_name} does not exist')

    # Task 3: Create Kafka Producer and Consumer
    @task(task_id='create_producer_consumer')
    def create_producer_consumer(logger_):

        prod = create_kafka_producer(logger_)
        cons = create_kafka_consumer('data_consumer', 'data_consumer')

        return prod, cons

    # Task 4: Get row count of res_properties table
    @task(task_id='postgres_data_count')
    def get_postgresql_rows(table_name_, remote=True):

        if remote is True:
            connection_str = os.getenv('POSTGRES_AWS_CONN')
            engine = create_engine(f"postgresql+psycopg2://{connection_str}:5432/gsmls")
        else:
            base, user, pw = get_us_pw('PostgreSQL')
            engine = create_engine(f'postgresql://{user}:{pw}@{base}:5432/{table_name_}')

        query = 'SELECT COUNT(*) FROM res_properties;'

        df = pd.read_sql(query, engine)

        return table_name, int(df.loc[0].values[0])

    # Task 5: Send pipeline initiation email
    @task(task_id='send_status_email')
    def status_email(table_name_, phase: str = 'Starting', **kwargs_):

        # https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html

        kafka_status = kwargs_['kafka_status']
        mongo_status_ = kwargs_['mongo_status']
        postgres_count = kwargs_['postgres_count']
        mongo_count = kwargs_['mongo_count']

        if phase == 'Starting':
            ip_address = os.getenv('DIGITAL_OCEAN_IP')
            subject = 'GSMLS Pipeline Has Started'
            message = f"""
                        start_time: {datetime.now()}
                        target_properties: {table_name_}
                        kafka_status: {kafka_status}
                        mongo_status: {mongo_status_}
                        postgres_count: {postgres_count}
                        mongo_count: {mongo_count}
                        
                        You can view the status and progress of your pipeline from the following ports:
                        - Airflow: http://{ip_address}:8085 → Airflow UI
                        - Spark: http://{ip_address}:8080 → Spark UI
                        - Mongo Express: http://{ip_address}:8081 → MongoDB Web Based UI
                        - pgAdmin: http://{ip_address}:5050 → PostgresSQL Web Based UI
                        - Selenium Browser: http://{ip_address}:7900 → Install VNC viewer for OS to view browser
                    """
        else:

            subject = 'GSMLS Pipeline Has Finished'
            message = f"""
                        end_time: {datetime.now()}
                        target_properties: {table_name_}
                        kafka_status: {kafka_status}
                        mongo_status: {mongo_status_}
                        postgres_count: {postgres_count}
                        mongo_count: {mongo_count}
                    """

        send_email(
            to='jqhholdingsllc@gmail.com',
            subject=subject,
            html_content=message
        )

    # Task 6: Create Kafka message sensor
    @task.sensor(poke_interval=300, timeout=3600, mode='reschedule')
    def new_msgs_available(topic: str, logger_) -> PokeReturnValue:

        offset_dict = {}
        # KafkaConsumer not thread safe, so I need to create one specifically for this task
        cons = create_kafka_consumer(f'{topic} msg_check', f'{topic} msg_check')

        # Check the partitions in the consumer. Returns a set of partition ids
        partitions = cons.partitions_for_topic(topic)

        if not partitions:
            logger_.warning(f"No partitions found for topic {topic}")

            raise AttributeError(f'No partitions created for {topic}')

        # Create list of TopicPartition objects to check end offsets
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        offset_dict.update({f'{tp}': False for tp in topic_partitions})

        while True not in list(offset_dict.keys()):
            # Loop through the available partitions to calculate lag between the committed offset and the end offset
            end_offsets = cons.end_offsets(topic_partitions)  # Returns a dict of partitions and their end offsets

            for tp in topic_partitions:
                # tp is the topic partition object
                committed = cons.committed(tp)
                latest = end_offsets[tp]

                if committed is None:
                    committed = 0
                lag = latest - committed
                logger_.info(f"Partition {tp.partition}: committed={committed}, latest={latest}, lag={lag}")

                if lag > 0:
                    offset_dict[tp] = True

            if True in list(offset_dict.values()):
                return PokeReturnValue(is_done=True)

    load_dotenv()
    logger = kwargs['logger']

    with TaskGroup(group_id='start_pipeline') as start_pipeline:

        # Task Dependencies, throw error if any of these don't work?
        kafka_conn = check_kafka_connection(logger)
        create_kafka_topics(kafka_conn)
        mongo_client = create_mongo_client()
        mongo_status, mongo_col, num_of_docs = check_mongodb(mongo_client, "realEstate", 'propertyImages', logger)
        producer, consumer = create_producer_consumer(logger)
        table_name, prop_count = get_postgresql_rows('res_properties')

        kwargs['kafka_status'] = kafka_conn
        kwargs['mongo_status'] = mongo_status
        kwargs['postgres_count'] = prop_count
        kwargs['mongo_count'] = num_of_docs
        status_email(table_name, **kwargs)

    with TaskGroup(group_id='etl_pipeline') as etl_pipeline:
        # Make sure to create function or have existing functions return the objects

        # This function should create the class then run the producer.
        # Task 5: Start the GSMLS message production
        PythonOperator(
            task_id='gsmls_producer',
            python_callable=GSMLS.airflow_gsmls_producer,
            op_kwargs={'producer': producer, 'table_name': 'res_properties'}
        )

        # The producer will publish data to both the data and image topics first
        kafka_msg_sensor = new_msgs_available('res_properties', logger).override(task_id='res_msgs_avail')
        kafka_img_sensor = new_msgs_available('prop_images', logger).override(task_id='image_msgs_avail')

        # Task 6: Start the GSMLS consumer
        gsmls_consumer = PythonOperator(
            task_id='gsmls_consumer',
            python_callable=KafkaGSMLSConsumer.main,  # This function should create the class then run it
            op_kwargs={'producer': consumer, 'table_name': 'res_properties'}
        )

        # Task 7: Start the MongoDB consumer
        mongo_consumer = PythonOperator(
            task_id='mongo_consumer',
            python_callable=RealEstateImages.main,  # This function should create the class then run it
            op_kwargs={'mongo_client': mongo_col}
        )

        # ETL Pipeline dependencies
        kafka_msg_sensor >> gsmls_consumer
        kafka_img_sensor >> mongo_consumer

    with TaskGroup(group_id='ending_pipeline') as ending_pipeline:

        # Close KafkaProducer connection
        # Close KafkaConsumer connection
        mongo_status, mongo_col, num_of_docs = check_mongodb(mongo_client, 'realEstate', 'propertyImages', logger)
        table_name, prop_count = get_postgresql_rows('res_properties')
        mongo_col.close()

        kwargs['phase'] = 'Ending'
        kwargs['kafka_status'] = False
        kwargs['mongo_status'] = False
        kwargs['postgres_count'] = prop_count
        kwargs['mongo_count'] = num_of_docs
        status_email(table_name, **kwargs)

    # Total pipeline dependencies
    start_pipeline() >> etl_pipeline() >> ending_pipeline()


if __name__ == '__main__':

    gsmls_pipeline()

