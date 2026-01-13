import os
import logging
import json
import time
import shelve
import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
from kafka.errors import NoBrokersAvailable

""" 
______________________________________________________________________________________________________________
                               Use this section to house the decorator functions
______________________________________________________________________________________________________________
"""


class TqdmLoggingHandler(logging.Handler):

    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)


def check_pipeline_metadata(pipeline, key=None, status=None):
    data_path = f"/app/pipeline_metadata"
    metadata_path = os.path.join(data_path, "metadata")

    if os.path.exists(metadata_path):
        with shelve.open(metadata_path) as data_file:
            pipelines = list(data_file.keys())

            if pipeline in pipelines:
                if status is not None:
                    data_file[pipeline][key] = status
                    print(f" ==== SAVING {key} STATUS OF {pipeline} TO {status} ==== ")
                else:
                    data_file[pipeline][key] = False
                    print(f" ==== RE-WRITING {key} STATUS OF {pipeline} TO {status} ==== ")
                data_file.sync()
            else:
                data_file[pipeline] = create_pipeline_metadata(pipeline)
                print(f" ==== INITIALIZING {key} STATUS OF {pipeline} TO {status} ==== ")

    else:
        with shelve.open(metadata_path) as data_file:
            data_file[pipeline] = create_pipeline_metadata(pipeline)
            print(f" ==== INITIALIZING {key} STATUS OF {pipeline} TO {status} ==== ")


def create_pipeline_metadata(pipeline):

    if pipeline == "gsmls_airflow_pipeline":
        return {"producer": False, "data_consumer": False,
                "image_consumer": False, "mongodb_start": None,
                "mongodb_final": None, "postgresql_start": None,
                "postgresql_final": None}

    elif pipeline == "gsmls_cleaning_pipeline":

        return {"cleaning": False,}

    elif pipeline == "gsmls_download_images":

        return {"download_images": False}


def create_postgres_connection(con_type: str, db_name=None):

    load_dotenv(get_filepath("env"))
    host = os.getenv("POSTGRES_AWS_HOST")
    port = 5432
    username = os.getenv("POSTGRES_AWS_USER")
    pw = os.getenv("POSTGRES_AWS_PASSWORD")

    if con_type == 'jdbc':
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"
        properties = {
            'user': username,
            'password': pw,
            'driver': "org.postgresql.Driver"
        }

        return jdbc_url, properties

    elif con_type == 'psycopg2':

        return None, {'user': username, 'password': pw, 'dbname': db_name, 'host': host, 'port': port}


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


def create_kafka_producer(client_id, logger=None, remote=True):
    retries = 0

    if remote is not True:
        bootstrap_servers = ["localhost:9092"]
    else:
        bootstrap_servers = ["broker-1:9092", "broker-2:9092", "broker-3:9092"]

    while retries <= 4:
        for attempt in range(1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=3,
                    acks="all",
                    client_id=client_id,
                )

                return producer

            except NoBrokersAvailable as nba:
                retries += 1

                if logger is not None:
                    logger.warning(f"{nba}")
                    logger.warning(
                        f"KafkaProducer couldn't be established on this attempt. This will be retry #{retries}"
                    )
                time.sleep(3)

                if retries > 4:
                    break

    raise NoBrokersAvailable


def create_mongodb_conn(remote=False):

    # Create a MongoDB connection
    if remote is False:
        connection_str = "mongodb://localhost:27017/"

    else:
        load_dotenv(get_filepath("env"))
        connection_str = os.getenv("ME_CONFIG_MONGODB_URL")

    return MongoClient(
            host=connection_str,
            serverSelectionTimeoutMS=5000,  # How long to wait when selecting a server (default 30s)
            maxPoolSize=100,  # Limit concurrent connections in the pool
            waitQueueTimeoutMS=5000,  # How long to wait for a free connection from pool
            retryWrites=True,  # Enable automatic retries for certain write operations
            retryReads=True,  # Enable automatic retries for certain read operations
            heartbeatFrequencyMS=10000,  # How often to check MongoDB availability
            connect=True,  # Force connection on client creation (fail fast if bad config)
        )


def create_selenium_webdriver(remote=True):

    options = Options()

    if remote is True:
        # Accessing Selenium container from Docker Compose in Digital Ocean Droplet
        load_dotenv(get_filepath("env"))
        ip_address = os.getenv("DIGITAL_OCEAN_IP")

        save_location = "/home/seluser/downloads"
        custom_user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0")

        s = {
            "savefile.default_directory": save_location,
            "download.default_directory": save_location,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
        }
        options.add_argument(f"user-agent={custom_user_agent}")
        options.add_experimental_option("prefs", s)

        return webdriver.Remote(
            command_executor=f"http://{ip_address}:4444/wd/hub", options=options
        )

    else:
        save_location = (
            "C:\\Users\\Username\\Desktop\\Selenium Temp Folder"  # May need to be changed
        )
        edge_profile_path = (
            "C:\\Users\\Username\\AppData\\Local\\Microsoft\\Edge\\User Data\\Default"
        )
        custom_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"

        s = {
            "savefile.default_directory": save_location,
            "download.default_directory": save_location,
            "download.prompt_for_download": False,
        }
        options.add_argument(f"user-data-dir={edge_profile_path}")
        options.add_argument(f"user-agent={custom_user_agent}")
        options.add_experimental_option("prefs", s)

        return webdriver.Edge(service=Service(), options=options)


def create_sql_engine(database: str, remote=True):

    if remote is True:
        load_dotenv(get_filepath("env"))
        connection_str = os.getenv("POSTGRES_AWS_CONN")
        engine = create_engine(f"postgresql+psycopg2://{connection_str}:5432/{database}", echo=False)

    else:
        base, user, pw = get_us_pw("PostgreSQL")
        engine = create_engine(f"postgresql://{user}:{pw}@{base}:5432/{database}", echo=False)

    return engine


def get_filepath(usecase: str):

    filepaths = {
        'downloads': ['/workspace/data/stage_one/downloads', '/app/downloads'],
        'env': ['/workspace/.env', '/app/.env', '/opt/airflow/.env'],
        'logger': ['/workspace/data/stage_one/logs', '/app/logs'],
        'backups': ['/workspace/consumer_backup_data', '/app/consumer_backup_data'],
        'metadata': ['/workspace/pipeline_metadata', '/app/pipeline_metadata']
    }

    for path in filepaths[usecase]:
        if os.path.exists(path):
            return path

    raise ValueError(f" ==== CURRENT FILEPATHS FOR {usecase} DO NOT EXIST IN THIS ENVIRONMENT ==== ")


def get_us_pw(website):
    """

    :param website:
    :return:
    """
    # Saves the current directory in a variable in order to switch back to it once the program ends
    previous_wd = os.getcwd()
    os.chdir("F:\\Add\\Folder\\Path")

    db = pd.read_excel("document_name.xlsx", index_col=0)
    username = db.loc[website, "Username"]
    pw = db.loc[website, "Password"]
    base_url = db.loc[website, "Base URL"]

    os.chdir(previous_wd)

    return username, base_url, pw


def logger_decorator(original_function):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(original_function.__name__)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        if not logger.handlers:
            # Create the FileHandler() and StreamHandler() loggers
            filepath = get_filepath("logger")
            log_filepath = os.path.join(
                filepath,
                original_function.__name__
                + " "
                + str(datetime.today().date())
                + ".log",
            )
            f_handler = logging.FileHandler(log_filepath)
            f_handler.setLevel(logging.DEBUG)
            c_handler = TqdmLoggingHandler()
            c_handler.setLevel(logging.INFO)
            # Create formatting for the loggers
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%d-%b-%y %H:%M:%S",
            )
            # Set the formatter for each handler
            f_handler.setFormatter(formatter)
            c_handler.setFormatter(formatter)
            logger.addHandler(f_handler)
            logger.addHandler(c_handler)

            kwargs["logger"] = logger
            kwargs["f_handler"] = f_handler
            kwargs["c_handler"] = c_handler

        result = original_function(*args, **kwargs)

        if result is None:
            pass
        else:
            return result

    return wrapper

