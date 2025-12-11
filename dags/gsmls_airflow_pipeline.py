import time
import json
import sys
import kafka.errors
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


def airflow_data_consumer(prop_type, logger, retry):

    consumer = KafkaGSMLSConsumer()
    results = consumer.main(prop_type, logger, retry)

    if not isinstance(results, bool):
        raise AirflowFailException
    else:
        return results


def airflow_image_consumer(prop_type='IMAGES', retry=False):

    img_consumer = KafkaGSMLSConsumer()
    results = img_consumer.main(prop_type, retry)

    if not isinstance(results, bool):
        raise AirflowFailException
    else:
        return results


def branching_decision(**kwargs):

    # starting_point returns True or False
    if starting_point(kwargs['prop_type']):

        # Returns the task_id based on the internal logic
        return cutoff_condition(kwargs['cutoff_time'], kwargs['tz'], kwargs['prop_type'])

    else:
        return f"skip_all_tasks_{kwargs['prop_type']}"


def condition_function(value):

    if value == 'complete':
        return False
    else:
        return True


def cutoff_condition(cutoff_time: dtime, tz, prop_type):

    now = pendulum.now(tz)
    start_hour = cutoff_time.hour
    start_mins = cutoff_time.minute

    start_time = now.replace(hour=start_hour, minute=start_mins)
    end_time = start_time + timedelta(hours=1, minutes=30)

    if start_time <= now <= end_time:
        return f'skip_all_tasks_{prop_type}'
    else:
        return f'start_pipeline_{prop_type}'


def cutoff_conversion(cutoff_time: dtime, tz):

    now = pendulum.now(tz)
    next_day = now + timedelta(days=1)

    cutoff_dt = next_day.replace(
        hour=cutoff_time.hour,
        minute=cutoff_time.minute
    )

    print(f" ==== THE CUTOFF TIME IS : {cutoff_dt} ==== ")
    return cutoff_dt


def progress_update(current_data: dict, table_name, prop_type):

    # Create database connections and pull preliminary data
    subject = "GSMLS Pipeline Update"
    engine = create_sql_engine("gsmls", remote=True)
    client = create_mongodb_conn(remote=True)
    database = client['realEstate']
    num_of_docs_ = database['propertyImages'].count_documents({})
    current_data["Documents"].append(num_of_docs_)

    try:
        # Scrape the event log for the latest saved event which occurred
        query = f"""
                    SELECT * FROM gsmls_event_log_new
                    WHERE id = (SELECT MAX(id) FROM gsmls_event_log_new)
                """
        df = pd.read_sql_query(query, con=engine.raw_connection())
        last_row = df.shape[0] - 1
        property_type = df.loc[last_row, "property_type"]

        assert prop_type == property_type

    except AssertionError:
        query = f"""
                SELECT * FROM gsmls_event_log_new
                WHERE property_type = '{prop_type}'
                ORDER BY id DESC
                LIMIT 1;
            """
        df = pd.read_sql_query(query, con=engine.raw_connection())
        last_row = df.shape[0] - 1
        property_type = df.loc[last_row, "property_type"]

    rows = 0
    town_check = df.loc[last_row, "municipality"]
    finished = df.loc[last_row, "finished"]
    date_produced = df.loc[last_row, "date_produced"]
    year = df.loc[last_row, "year_"]
    county = df.loc[last_row, "county"]
    split_type = df.loc[last_row, "split_type"]
    split_index = df.loc[last_row, "split_index"]
    current_data["Municipality"].append(town_check)
    current_data["Split_Type"].append(split_type)
    current_data["Split_Index"].append(split_index)

    message = f"""
            <br>
            <b>Program Progress</b><br>
            <b>Current Year</b>: {year}<br>
            <b>Last Scraped Municipality</b>: {town_check}<br>
            <b>County</b>: {county}<br>
            <b>Finished</b>: {finished}<br>
            <b>Current Data Produced</b>: {rows} Rows<br>
            <b>MongoDB Document Count</b>: {num_of_docs_}<br>
            <b>Property Type</b>: {property_type}<br<br>
            """

    # If the date of the data produced and today doesn't match, no data has been scraped today
    if (date_produced != datetime.now().date()) or len(current_data["Municipality"]) == 1:

        current_data["County"].append(county)
        current_data["Year"].append(year)
        current_data["Rows"].append(rows)

    else:
        query = f"""
            SELECT 
                cnt.total_count,
                last_row."STATUS_SHORT",
                last_row."TOWN",
                last_row."COUNTY",
                last_row."YEAR",
                last_row."EXPIREDATE",
                last_row."WITHDRAWNDATE"
            FROM
                (SELECT COUNT("MLSNUM") AS total_count
                 FROM {table_name}
                 WHERE "SCRAPED_DATE" >= '{datetime.now().date()}') AS cnt
            CROSS JOIN
                (SELECT "STATUS_SHORT", "TOWN", "COUNTY", "YEAR", "EXPIREDATE", "WITHDRAWNDATE"
                 FROM {table_name}
                 WHERE "SCRAPED_DATE" >= '{datetime.now().date()}'
                 ORDER BY "COUNTY" DESC, "TOWN" DESC
                 LIMIT 1) AS last_row;
        """

        df = pd.read_sql_query(query, con=engine.raw_connection())

        if not df.empty:
            last_row = df.shape[0] - 1

            town = df.loc[last_row, "TOWN"]
            current_data["Municipality"][-1] = town
            county = df.loc[last_row, "COUNTY"]
            current_data["County"].append(county)
            rows = len(df)
            current_data["Rows"].append(rows)
            year = df.loc[last_row, "YEAR"]
            current_data["Year"].append(year)
            status = df.loc[last_row, "STATUS_SHORT"]
            delta = rows - current_data["Rows"][-2]

            if year == 0:
                if status in ['WD', 'W']:
                    year = df.loc[last_row, "WITHDRAWNDATE"].split('/')[2][:4]
                elif status in ['XD', 'X']:
                    year = df.loc[last_row, "EXPIREDATE"].split('/')[2][:4]

            message = f"""
                    <br>
                    <b>Program Location</b>
                    <b>Current Year</b>: {year}<br>
                    <b>Municipality</b>: {town}<br>
                    <b>County</b>: {county}<br>
                    <b>Current Data Produced</b>: {rows} Rows<br>
                    <b>MongoDB Document Count</b>: {num_of_docs_}<br>
                    <b>Data Added Since Last Update</b>: {delta}<br><br>
            """

        else:
            current_data["County"].append(county)
            current_data["Rows"].append(rows)
            current_data["Year"].append(year)

    if len(current_data["Municipality"]) == 1:
        send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
        return False
    elif current_data["Rows"][-1] != current_data["Rows"][-2]:
        send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
        return False
    elif current_data["Municipality"][-1] != current_data["Municipality"][-2]:
        send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
        return False
    elif current_data["Municipality"][-1] == current_data["Municipality"][-2]:
        if current_data["Split_Type"][-1] != current_data["Split_Type"][-2]:
            send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
        elif current_data["Split_Type"][-1] == current_data["Split_Type"][-2]:
            if current_data["Split_Index"][-1] != current_data["Split_Index"][-2]:
                send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
            elif current_data["Split_Index"][-1] == current_data["Split_Index"][-2]:
                if finished == 'No':
                    send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)
                else:
                    return True


def skip_pipeline():
    raise AirflowSkipException("Time cutoff reached - skipping full pipeline")


def starting_point(prop_type):

    engine = create_sql_engine("gsmls", remote=True)

    query = f"""
                SELECT * FROM gsmls_event_log_new
                WHERE property_type = '{prop_type.upper()}'
                ORDER BY id DESC
                LIMIT 1;
                """

    metadata = pd.read_sql_query(query, con=engine.raw_connection())
    last_row = metadata.shape[0] - 1

    if metadata.empty:
        print(f" ==== DATA DOESN'T EXIST. BEGINNING {prop_type} SCRAPE ==== ")
        return True

    else:
        last_scraped_county = metadata.loc[last_row, "county"]
        last_scraped_muni = metadata.loc[last_row, "municipality"]
        finished = metadata.loc[last_row, "finished"]
        date_produced = metadata.loc[last_row, "date_produced"]
        timeframe = metadata.loc[last_row, "timeframe"]
        delta = datetime.now() - date_produced
        print(f' ==== TIME DELTA: {delta}')

        # All data for the last property type was acquired
        if last_scraped_county == 30 and last_scraped_muni == "White Twp." and finished == "Yes":

            if timeframe in ["historic", "mixed"]:
                return True
            elif delta.days <= 6:
                print(f' ==== NO NEW DATA AVAILABLE FOR {prop_type.upper()}==== ')
                return False
            else:
                print(f' ==== BEGINNING {prop_type} SCRAPE ==== ')
                return True

        else:
            print(f' ==== BEGINNING {prop_type} SCRAPE ==== ')
            return True


def new_msgs_available(topic, logger_):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic}_msg_check", "data_consumer")

    # Check the partitions in the consumer. Returns a set of partition ids
    partitions = cons.partitions_for_topic(topic)
    print(f'{partitions}')

    try:
        if not partitions:
            logger_.warning(f"No partitions found for topic {topic}")

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
            logger_.info(
                f"Partition {tp.partition}: committed={committed}, latest={latest}, lag={lag}"
            )

            if lag > 0:
                offset_dict[tp] = True

        if True in list(offset_dict.values()):
            logger_.info(f"New data found for {topic}")

            return True
        else:
            return False

    except AttributeError:
        logger_.info(f"No partitions found for topic {topic}")

        return False


def airflow_gsmls(prop_type, **kwargs):

    obj = GSMLS(prop_type)
    kwargs['property_type'] = prop_type
    kwargs['logger'].info(f'{obj.__dict__}')
    kwargs['logger'].info('==== ETL STARTED ====')
    results = obj.airflow_gsmls_producer(**kwargs)
    kwargs['logger'].info('==== ETL ENDED ====')

    if not isinstance(results, int):
        raise AirflowFailException
    else:
        return results


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
def status_email(postgres_results, mongo_results, phase: str = "Starting", rows_added=None):

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
                    -- <b>Airflow</b>: http://{ip_address}:8085<br>
                    -- <b>Spark</b>: http://{ip_address}:8080<br>
                    -- <b>pgAdmin</b>: http://{ip_address}:5050 (pgAdmin)<br>
                    -- <b>Selenium</b>: http://{ip_address}:7900 <br>
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
                    <b>Postgres Rows Added</b>: {rows_added}<br><br>
                """

    send_email(to="nj.realestate.pybot@gmail.com", subject=subject, html_content=message)


# Define default args
eastern = timezone("America/New_York")
default_args = {
    "owner": "Jibreel Hameed",
    "email": ['nj.realestate.pybot@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime(2025, 11, 17, hour=9, minute=30, tzinfo=eastern),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    }

prop_type_dict = {
        'RES': 'res_properties',
        'MUL': 'mul_properties',
        # 'LND': 'lnd_properties',
        # 'RNT': 'rnt_properties',
        # 'TAX': 'tax_properties'
    }


# Define DAG as decorator over final pipeline function
@dag(
    "GSMLS_Scrape_And_Preprocessing",
    description="",
    default_args=default_args,
    schedule=timedelta(days=1),
)
@logger_decorator
def gsmls_pipeline(**kwargs):

    load_dotenv()
    logger = kwargs["logger"]
    f_handler = kwargs["f_handler"]
    c_handler = kwargs["c_handler"]
    previous_group = None
    cutoff_time = dtime(hour=2, minute=30, tzinfo=eastern)
    progress_tracker = {
        'Year': [],
        'Municipality': [],
        'County': [],
        'Rows': [],
        'Documents': [],
        'Split_Type': [],
        'Split_Index': []
    }

    # Check the Kafka connection
    kafka_conn = check_kafka_connection(logger)
    mongo_start_results = check_mongodb("realEstate", "propertyImages", logger)
    # Need to adjust starting point. The DB I/O is causing issues during DAG parsing which corrupts the runs
    # property_list = starting_point()

    for prop_type, topic in prop_type_dict.items():

        branch_decision = BranchPythonOperator(
            task_id=f'branching_decision_{prop_type.lower()}',
            python_callable=branching_decision,
            op_kwargs={'cutoff_time': cutoff_time,
                       'tz': eastern,
                       'prop_type': prop_type.lower()},
            trigger_rule="none_failed"
        )

        skip_all_tasks = PythonOperator(
            task_id=f"skip_all_tasks_{prop_type.lower()}",
            python_callable=skip_pipeline,
            trigger_rule="all_success"
        )

        with TaskGroup(group_id=f"start_pipeline_{prop_type.lower()}") as start_pipeline:

            # Create Kafka topics if necessary
            _ = create_kafka_topics(logger, topic=topic, status=kafka_conn)
            postgresql_results1 = get_postgresql_rows(topic)
            status_email(postgresql_results1, mongo_start_results)

        with TaskGroup(group_id=f"etl_pipeline_{prop_type.lower()}") as etl_pipeline:
            # Update so table_name and prop type isn't hard-coded
            # Task 5: Start the GSMLS message production

            rows_extracted = PythonOperator(
                task_id="gsmls_producer",
                python_callable=airflow_gsmls,
                op_kwargs={"prop_type": prop_type, 'logger': logger,
                           "f_handler": f_handler, "c_handler": c_handler,
                           "cutoff_time": cutoff_conversion(cutoff_time, eastern)})
            # The producer will publish data to both the data and image topics first
            # Task decorators don't define __rshift__ ">>" so I need to use classic Operators for dependencies
            kafka_msg_sensor = PythonSensor(
                task_id="kafka_msg_sensor",
                python_callable=new_msgs_available,
                op_kwargs={'topic': topic, 'logger_': logger},
                poke_interval=60,
                timeout=3600,
                mode="reschedule"
            )

            kafka_img_sensor = PythonSensor(
                task_id=f"kafka_image_sensor",
                python_callable=new_msgs_available,
                op_kwargs={'topic': 'prop_images', 'logger_': logger},
                poke_interval=60,
                timeout=3600,
                mode="reschedule"
            )

            PythonSensor(
                task_id=f"progress_sensor",
                python_callable=progress_update,
                op_kwargs={'table_name': topic, 'current_data': progress_tracker,
                           'prop_type': prop_type},
                poke_interval=1800,
                timeout=86400,
                mode="reschedule"
            )

            # Task 6: Start the GSMLS consumer and MongoDB consumer
            gsmls_consumer = PythonOperator(
                task_id=f"gsmls_consumer",
                python_callable=airflow_data_consumer,
                op_kwargs={"prop_type": prop_type, "logger": logger, "retry": False})

            image_consumer = PythonOperator(
                task_id=f"image_consumer",
                python_callable=airflow_image_consumer,
                op_kwargs={"retry": False})

            # ETL Pipeline dependencies
            kafka_msg_sensor >> gsmls_consumer
            # kafka_msg_sensor >> progress_sensor
            kafka_img_sensor >> image_consumer

        merge = EmptyOperator(
            task_id=f"merge_tasks_{prop_type.lower()}",
            trigger_rule="none_failed_min_one_success"
        )

        with TaskGroup(group_id=f"ending_pipeline_{prop_type.lower()}") as ending_pipeline:

            mongo_end_results = check_mongodb("realEstate", "propertyImages", logger)
            postgresql_results2 = get_postgresql_rows(topic)
            status_email(postgresql_results2, mongo_end_results, phase='Ending', rows_added=rows_extracted)

        # # Total pipeline dependencies
        branch_decision >> skip_all_tasks
        branch_decision >> start_pipeline
        start_pipeline >> etl_pipeline >> ending_pipeline
        skip_all_tasks >> merge
        ending_pipeline >> merge

        if previous_group:
            previous_group >> branch_decision

        previous_group = ending_pipeline


# dag_instance = gsmls_pipeline()
gsmls_pipeline()

