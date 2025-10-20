import os
import logging
import json
import time
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


def create_kafka_consumer(client_id, group_id):

    return KafkaConsumer(
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=["broker-1:9092", "broker-2:9092", "broker-3:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=120000,
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

    mongo_conn_dict = {
        'host': '',
        'serverSelectionTimeoutMS': 5000,  # How long to wait when selecting a server (default 30s)
        'maxPoolSize': 100,  # Limit concurrent connections in the pool
        'waitQueueTimeoutMS': 5000,  # How long to wait for a free connection from pool
        'retryWrites': True,  # Enable automatic retries for certain write operations
        'retryReads': True,  # Enable automatic retries for certain read operations
        'heartbeatFrequencyMS': 10000,  # How often to check MongoDB availability
        'connect': True,  # Force connection on client creation (fail fast if bad config)
    }

    # Create a MongoDB connection
    if remote is False:
        mongo_conn_dict['host'] = "mongodb://localhost:27017/"

    else:
        load_dotenv("/opt/airflow/.env")
        connection_str = os.getenv("ME_CONFIG_MONGODB_URL")
        mongo_conn_dict['host'] = connection_str

    return mongo_conn_dict


def create_selenium_webdriver(remote=True):

    options = Options()

    if remote is True:
        # Accessing Selenium container from Docker Compose in Digital Ocean Droplet
        load_dotenv("/opt/airflow/.env")
        ip_address = os.getenv("DIGITAL_OCEAN_IP")
        return webdriver.Remote(
            command_executor=f"http://{ip_address}:4444/wd/hub", options=options
        )

    else:
        save_location = (
            "C:\\Users\\Omar\\Desktop\\Selenium Temp Folder"  # May need to be changed
        )
        edge_profile_path = (
            "C:\\Users\\Omar\\AppData\\Local\\Microsoft\\Edge\\User Data\\Default"
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
        load_dotenv("/opt/airflow/.env")
        connection_str = os.getenv("POSTGRES_AWS_CONN")
        engine = create_engine(
            f"postgresql+psycopg2://{connection_str}:5432/{database}"
        )
    else:
        base, user, pw = get_us_pw("PostgreSQL")
        engine = create_engine(f"postgresql://{user}:{pw}@{base}:5432/{database}")

    return engine


def logger_decorator(original_function):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(original_function.__name__)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        if not logger.handlers:
            # Create the FileHandler() and StreamHandler() loggers
            if not os.path.exists("/opt/airflow/logs/logger_decorator"):
                os.mkdir("/opt/airflow/logs/logger_decorator")
            filepath = "/opt/airflow/logs/logger_decorator"
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
