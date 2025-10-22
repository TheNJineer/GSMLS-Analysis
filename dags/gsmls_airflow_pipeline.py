import time
import json
import sys

import kafka.errors
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from plugins.utility_func import logger_decorator, create_kafka_consumer
from plugins.utility_func import create_sql_engine, create_kafka_producer, create_mongodb_conn
from airflow.sdk import task, dag, PokeReturnValue, TaskGroup
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator
from kafka import KafkaClient
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from kafka.admin.client import KafkaAdminClient
from kafka.cluster import ClusterMetadata
from pymongo.errors import ConnectionFailure
from plugins.GSMLS import GSMLS
from plugins.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from plugins.RealEstateImages import RealEstateImages

# That guarantees the DAG loader process can find the plugins module even if
# Airflow ignores the PYTHONPATH
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))

# Define default args
default_args = {
    "owner": "Jibreel Hameed",
    "start_date": datetime(2025, 10, 19),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def airflow_gsmls_producer(data_producer, table_name):

    obj = GSMLS()
    obj.airflow_gsmls_producer(data_producer=data_producer, table_name=table_name)


def airflow_image_consumer(remote_client):

    consumer = RealEstateImages()
    consumer.main(remote_client=remote_client)


def airflow_data_consumer(data_consumer, img_producer, table_name):

    engine = create_sql_engine("gsmls", remote=True)
    img_consumer = KafkaGSMLSConsumer(connection=engine, producer_=img_producer)
    img_consumer.main(remote_consumer=data_consumer)


# Define DAG as decorator over final pipeline function
@dag(
    "GSMLS_Pipeline",
    description="",
    default_args=default_args,
    schedule=timedelta(days=7),
)
@logger_decorator
def gsmls_pipeline(**kwargs):

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

            for t in topic_list:

                if t not in available_topics:

                    topic_obj = NewTopic(
                        name=topic,
                        num_partitions=3,
                        replication_factor=2,
                        topic_configs={"cleanup.policy": "compact"},  # Look into what other configs I need
                    )
                    try:
                        admin_client.create_topics(
                            new_topics=[topic_obj], validate_only=False
                        )
                    except kafka.errors.TopicAlreadyExistsError:
                        logger_.info(f'Topic {t} already exists')
                    else:
                        logger_.info(f'Topic {topic} created in Apache Kafka topic list')

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
            return {'mongo_status': True, 'mongo_col': table_result, 'num_of_docs': num_of_docs_}
        else:
            return {'mongo_status': True, 'mongo_col': table_result, 'num_of_docs': num_of_docs_}

    # Task 3: Create Kafka Producer and Consumer
    @task(task_id="create_producer_consumer", multiple_outputs=True)
    def create_producer_consumer(logger_):

        data_prod = create_kafka_producer("data_producer", logger=logger_)
        image_prod = create_kafka_producer("image_producer", logger=logger_)
        cons = create_kafka_consumer("data_consumer", "data_consumer")

        return {'data_producer': data_prod, 'image_producer': image_prod, 'consumer': cons}

    # Task 4: Get row count of res_properties table
    @task(task_id="postgres_data_count", multiple_outputs=True)
    def get_postgresql_rows(table_name_, remote=True):

        engine = create_sql_engine("gsmls", remote=remote)

        query = f"SELECT COUNT(*) FROM {table_name_};"

        with engine.connect() as connection:
            df = pd.read_sql_query(query, con=connection)

        return {'table_name': table_name_, 'prop_count': int(df.loc[0].values[0])}

    # Task 5: Send pipeline initiation email
    @task(task_id="send_status_email")
    def status_email(table_name_, phase: str = "Starting", **kwargs_):

        # https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html

        kafka_status = kwargs_["kafka_status"]
        mongo_status_ = kwargs_["mongo_status"]
        postgres_count = kwargs_["postgres_count"]
        mongo_count = kwargs_["mongo_count"]

        if phase == "Starting":
            ip_address = os.getenv("DIGITAL_OCEAN_IP")
            subject = "GSMLS Pipeline Has Started"
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

            subject = "GSMLS Pipeline Has Finished"
            message = f"""
                        end_time: {datetime.now()}
                        target_properties: {table_name_}
                        kafka_status: {kafka_status}
                        mongo_status: {mongo_status_}
                        postgres_count: {postgres_count}
                        mongo_count: {mongo_count}
                    """

        send_email(to="jqhholdingsllc@gmail.com", subject=subject, html_content=message)

    # Task 6: Create Kafka message sensor
    @task.sensor(poke_interval=300, timeout=3600, mode="reschedule")
    def new_msgs_available(topic: str, logger_) -> PokeReturnValue:

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

        while True not in list(offset_dict.keys()):
            # Loop through the available partitions to calculate lag between the committed offset and the end offset
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
                return PokeReturnValue(is_done=True)

    load_dotenv()
    logger = kwargs["logger"]

    with TaskGroup(group_id="start_pipeline") as start_pipeline:

        # Task Dependencies, throw error if any of these don't work?
        kafka_conn = check_kafka_connection(logger)
        # _ = create_kafka_topics(logger, topic="res_properties", status=kafka_conn)
        _ = create_kafka_topics(logger, topic="res_properties", status=True)
        mongo_start_results = check_mongodb("realEstate-cloud", "propertyImages", logger)
        # kafka_objects = create_producer_consumer(logger)
        postgresql_results1 = get_postgresql_rows("res_properties")

        kwargs["kafka_status"] = kafka_conn
        kwargs["mongo_status"] = mongo_start_results['mongo_status']
        kwargs["postgres_count"] = postgresql_results1['prop_count']
        kwargs["mongo_count"] = mongo_start_results['num_of_docs']
        status_email(postgresql_results1['table_name'], **kwargs)

    # with TaskGroup(group_id="etl_pipeline") as etl_pipeline:
    #     # Make sure to create function or have existing functions return the objects
    #
    #     # This function should create the class then run the producer.
    #     # Task 5: Start the GSMLS message production
    #     PythonOperator(
    #         task_id="gsmls_producer",
    #         python_callable=airflow_gsmls_producer,
    #         op_kwargs={"data_producer": kafka_objects['data_producer'],
    #                   "table_name": postgresql_results1['table_name']},
    #     )
    #
    #     # The producer will publish data to both the data and image topics first
    #     kafka_msg_sensor = new_msgs_available(postgresql_results1['table_name'], logger).override(
    #         task_id="res_msgs_avail"
    #     )
    #     kafka_img_sensor = new_msgs_available("prop_images", logger).override(
    #         task_id="image_msgs_avail"
    #     )
    #
    #     # Task 6: Start the GSMLS consumer
    #     gsmls_consumer = PythonOperator(
    #         task_id="gsmls_consumer",
    #         python_callable=airflow_data_consumer,
    #         op_kwargs={
    #             "data_consumer": kafka_objects['consumer'],
    #             "img_producer": kafka_objects['image_producer'],
    #             "table_name": postgresql_results1['table_name'],
    #         },
    #     )
    #
    #     # Task 7: Start the MongoDB consumer
    #     mongo_consumer = PythonOperator(
    #         task_id="mongo_consumer",
    #         python_callable=airflow_image_consumer,
    #         op_kwargs={"remote_client": mongo_start_results['mongo_col']},
    #     )
    #
    #     # ETL Pipeline dependencies
    #     kafka_msg_sensor >> gsmls_consumer
    #     kafka_img_sensor >> mongo_consumer
    #
    with TaskGroup(group_id="ending_pipeline") as ending_pipeline:

        # Close KafkaProducer connection
        # Close KafkaConsumer connection
        mongo_end_results = check_mongodb("realEstate", "propertyImages", logger)
        postgresql_results2 = get_postgresql_rows("res_properties")
        # mongo_end_results['mongo_col'].close()

        kwargs["phase"] = "Ending"
        kwargs["kafka_status"] = False
        kwargs["mongo_status"] = False
        kwargs["postgres_count"] = postgresql_results2['prop_count']
        kwargs["mongo_count"] = mongo_end_results['num_of_docs']
        status_email(postgresql_results2['table_name'], **kwargs)
    #
    # Total pipeline dependencies
    # start_pipeline >> etl_pipeline >> ending_pipeline
    start_pipeline >> ending_pipeline


gsmls_pipeline()
