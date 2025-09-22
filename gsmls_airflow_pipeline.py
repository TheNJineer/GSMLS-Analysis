import time
import json
import pandas as pd
from datetime import datetime
from datetime import timedelta
from utility_func import get_pw, logger_func
from airflow.sdk import task, dag
from airflow.providers.standard.operators.email import EmailOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.admin.client import KafkaAdminClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from sqlalchemy import create_engine
from GSMLS import main as gsmls_main
from GSMLS_KafkaConsumer import main as kafka_consumer
from ImageConsumer import mongo_consumer
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
@logger_func
@dag('GSMLS_Pipeline', description='', default_args=default_args, schedule=timedelta(days=7))
def gsmls_pipeline(**kwargs):

    # Task 1: Check the health of Apache Kafka #Connection
    @task(task_id='check_kafka_connection')
    def check_kafka_connection(logger_):

        # Make sure these brokers are created in the #Docker Compose yaml
        brokers_ready={1: False, 2: False, 3: False}
        client_ready = False

        admin_client = KafkaClient(bootstrap_servers=['broker-1:9092', 'broker-2:9092', 'broker-3:9092'],
                                   client_id='health_check')
        admin_client.poll(timeout_ms=1000)

        # Step 1: Broad-level check that the overall client is initialized
        while client_ready is False:
            if admin_client.ready() is True:
                client_ready = True

        # Step 2: Individual broker checks
        while (brokers_ready[1] | brokers_ready[2] | brokers_ready[3]) is False:

            for id_ in brokers_ready.keys():

                conn_result = admin_client.is_ready(node=id_)
                brokers_ready[id_] = conn_result

            if list(brokers_ready.values()).count(True) < 3:
                # Need this to be able to check if more than one node isn’t connected
                unconnected_node= list(brokers_ready.values()).index(False)
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
    def check_mongodb(client, logger_):

        retries = 0

        while retries < 10:

            try:
                conn_result = client.admin.command('ping')
            except ConnectionFailure as cf:
                logger_.warning(f"{cf}")
                time.sleep(3)
                retries += 1
            else:

                try:
                    table_name_ = 'Table_name'
                    table_result = 'Does the table exist?' # Check if table is available
                    num_of_docs_ = 'How many documents exists in the table?' # Check the amount of documents

                    if conn_result is True and table_result is True:
                        return True, num_of_docs_

                except ConnectionFailure as cf:
                    logger_.warning(f"{cf}")
                    raise ConnectionFailure(f'Table {table_name_} does not exist')

        else:
            return False, 0

    # Task 3: Create Kafka Producer and Consumer
    @task(task_id='create_producer_consumer')
    def create_producer_consumer(logger_):

        prod = create_kafka_producer(logger_)
        cons = create_kafka_consumer('data_consumer', 'data_consumer')

        return prod, cons

    # Task 4: Get row count of res_properties table
    @task(task_id='postgres_data_count')
    def get_postgresql_rows(table_name_):

        base, user, pw = get_pw('PostgreSQL')
        engine = create_engine(f'postgresql://{user}:{pw}@{base}:5432/{table_name_}')

        query = 'SELECT COUNT(*) FROM res_properties;'

        df = pd.read_sql(query, engine)

        return table_name, int(df.loc[0].values[0])

    # Task 5: Send pipeline initiation email
    @task(task_id='send_status_email')
    def send_email(table_name_, phase: str = 'Starting', **kwargs):

        kafka_status = kwargs['kafka_status']
        mongo_status = kwargs['mongo_status']
        postgres_count = kwargs['postgres_count']
        mongo_count = kwargs['mongo_count']

        if phase == 'Starting':

            subject = 'GSMLS Pipeline Has Started'
            message = f"""
                        start_time: {datetime.now()},
                        target_properties: {table_name_},
                        kafka_status: {kafka_status},
                        mongo_status: {mongo_status},
                        postgres_count: {postgres_count}
                        mongo_count: {mongo_count}
                        
                        You can view the status and progress of your pipeline from the following ports:
                        - Airflow: http://167.172.245.142:8085 → Airflow UI
                        - Spark: http://167.172.245.142:8080 → Spark UI
                        - Mongo Express: http://167.172.245.142:8081 → MongoDB Web Based UI
                        - pgAdmin: http://167.172.245.142:5050 → PostgresSQL Web Based UI
                    """
        else:

            subject = 'GSMLS Pipeline Has Finished'
            message = f"""
                        end_time: {datetime.now()},
                        target_properties: {table_name_},
                        kafka_status: {kafka_status},
                        mongo_status: {mongo_status},
                        postgres_count: {postgres_count}
                        mongo_count: {mongo_count}
                    """

        EmailOperator(
            to='jqhholdingsllc@gmail.com',
            subject=subject,
            html_content=message
        )

    # Task 6: Create Kafka message sensor
    @task(task_id='new_msg_available')
    def new_msgs_available(topic, logger_):

        offset_dict = {}
        # KafkaConsumer not thread safe, so I need to create one specifically for this task
        cons = create_kafka_consumer('msg_check', 'msg_check')

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
                return True

    logger = kwargs['logger']

    with TaskGroup(group_id='start_pipeline') as start_pipeline:

        # Task Dependencies, throw error if any of these don't work?
        kafka_conn = check_kafka_connection(logger)
        create_kafka_topics(kafka_conn)
        mongo_client = create_mongo_client()
        mongo_conn, num_of_docs = check_mongodb(mongo_client, logger)
        producer, consumer = create_producer_consumer(logger)
        table_name, prop_count = get_postgresql_rows('res_properties')
        # Does the email need to include the status of the topics as well?
        send_email(table_name, phase='Starting', kafka_status=kafka_conn,
                   mongo_status=mongo_conn, postgres_count=prop_count,
                   mongo_count=num_of_docs)

    with TaskGroup(group_id='etl_pipeline') as etl_pipeline:
        # Make sure to create function or have existing functions return the objects

        # This function should create the class then run the producer.
        # Needs to accept KafkaProducer obj and PostgreSQl table name
        # Task 5: Start the GSMLS message production
        gsmls_producer = PythonOperator(
            task_id='gsmls_producer',
            python_callable=gsmls_main,
            op_kwargs={'producer':producer, 'table_name': 'res_properties'}
        )

        # Task 5a: Have a sensor here that says messages are in the topic
        kafka_msg_sensor = new_msgs_available('res_properties', logger)

        # Task 6: Start the GSMLS consumer
        gsmls_consumer = PythonOperator(
            task_id='gsmls_consumer',
            python_callable=kafka_consumer # This function should create the class then run it
        )

        # Task 6a: Have a sensor here that says images are in the topic
        kafka_img_sensor = new_msgs_available('prop_images', logger)

        # Task 7: Start the MongoDB consumer
        mongo_consumer = PythonOperator(
            task_id='mongo_consumer',
            python_callable=mongo_consumer, # This function should create the class then run it
            op_kwargs={'mongo_client':mongo_conn}
        )

        # ETL Pipeline dependencies
        gsmls_producer()
        kafka_msg_sensor >> gsmls_consumer
        kafka_img_sensor >> mongo_consumer

    with TaskGroup(group_id='ending_pipeline') as ending_pipeline:

        # Close KafkaProducer connection
        # Close KafkaConsumer connection
        mongo_conn, num_of_docs = check_mongodb(mongo_client)
        table_name, prop_count = get_postgresql_rows('res_properties')
        # Close MongoDB connection
        send_email(table_name, phase='Ending', kafka_status=kafka_conn,
                   mongo_status=mongo_conn, postgres_count=prop_count,
                   mongo_count=num_of_docs)

    # Total pipeline dependencies
    start_pipeline >> etl_pipeline >> ending_pipeline

if __name__ == '__main__':

    gsmls_pipeline()