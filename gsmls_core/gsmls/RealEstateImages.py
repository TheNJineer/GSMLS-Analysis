import shelve
import os
import re
import sys
import requests
import random
import time
import boto3
import pendulum
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from pendulum import timezone
from pprint import pformat
from pprint import pprint
from tqdm.auto import tqdm
from collections import defaultdict
from io import BytesIO
from requests import Session
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
from pymongo.errors import CursorNotFound
from botocore.exceptions import ClientError
from urllib3.exceptions import ProtocolError, IncompleteRead
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, ConnectionError, SSLError, ProxyError, HTTPError
from gsmls.utility_func import logger_decorator, create_sql_engine, create_mongodb_conn
from gsmls.utility_func import get_filepath, check_pipeline_metadata, current_status


class RealEstateImages:

    def __init__(self, db_name="realEstate", col_name="propertyImages",
                 latest_order_num=None, local=False, remote=True):
        self.db_name = db_name
        self.col_name = col_name
        self.sql_conn = create_sql_engine("nj_tax_assessor", remote=remote)
        if local is False:
            self.mongo_db_conn = create_mongodb_conn(remote=remote)
            self.database = self.check_for_database()
            self.collection = self.check_for_collection()
        else:
            self.mongo_db_conn = create_mongodb_conn(remote=False)
            self.database = self.check_for_database()
            self.collection = self.check_for_collection()
        self.proxy_check_time = datetime.now()
        self.total_props = 0
        self.total_images = 0
        self.isp_ips = None
        self.dead_ips = []
        self.ip_api = RealEstateImages.load_ip_api()
        self.latest_isp_order_num = latest_order_num
        self.static_ip_status = "active"
        self.proxy_manager()
        self.image_dir = "/opt/airflow/MLS Photos"
        self.home_sections = {
            "Bathroom": re.compile(
                r"bath(\s)?room|bath|powder|master bath", flags=re.IGNORECASE
            ),
            "Bedroom": re.compile(
                r"bed(\s)?room|bed|master suite|master br|master bedrm",
                flags=re.IGNORECASE,
            ),
            "Kitchen": re.compile("kitchen|breakfast", flags=re.IGNORECASE),
            "Garage": re.compile("garage", flags=re.IGNORECASE),
            "Front": re.compile(r"front yard|front(\sexterior)?", flags=re.IGNORECASE),
            "Entrance": re.compile("entrance", flags=re.IGNORECASE),
            "Foyer": re.compile("foyer", flags=re.IGNORECASE),
            "Laundry": re.compile(
                r"laundry(\sroom)?|washer|dryer", flags=re.IGNORECASE
            ),
            "Backyard": re.compile(
                r"back(\s)?yard|rear(\sexterior)?|yard", flags=re.IGNORECASE
            ),
            "Living Room": re.compile(
                r"living(\sroom)?|family(\sroom)?|liv rm|family rm", flags=re.IGNORECASE
            ),
            "Basement": re.compile(
                "basement|recreation|rec|lower level|bsmt", flags=re.IGNORECASE
            ),
            "Gym": re.compile(r"exercise(\sroom)?|gym(\sroom)?", flags=re.IGNORECASE),
            "Attic": re.compile("attic", flags=re.IGNORECASE),
            "Office": re.compile("office|den", flags=re.IGNORECASE),
            "Deck": re.compile("deck|patio", flags=re.IGNORECASE),
            "Pool": re.compile("pool", flags=re.IGNORECASE),
            "Driveway": re.compile("driveway|parking", flags=re.IGNORECASE),
            "Dining Room": re.compile(r"dining(\sroom)?", flags=re.IGNORECASE),
            "Porch": re.compile("porch", flags=re.IGNORECASE),
            "Floor Plans": re.compile("floor plan(s)?", flags=re.IGNORECASE),
            "Tax Map": re.compile(r"(tax\s)?map", flags=re.IGNORECASE),
            "Sun Room": re.compile(r"sun(\s)?room|solarium", flags=re.IGNORECASE),
            "Alternates": re.compile(
                "Image of listing|Image of listing.*", flags=re.IGNORECASE
            ),
        }

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    def alternates_image_capture(self, image_num, imagedict, **kwargs):
        """
        REFACTOR
        """

        if ((kwargs["Section_Type"] is not None)
                and ("Image of listing" == kwargs["Section"])
                and (image_num == 0)):

            self.capture_front_image_url(image_num, imagedict, **kwargs)

        elif ("Image of listing" in kwargs["Section"]) and (image_num >= 0):

            for section, pattern in self.home_sections.items():
                try:
                    if pattern.search(kwargs["Section"][16:]) is not None:

                        if section != "Alternates":

                            if section == "Front":

                                self.capture_front_image_url(image_num, imagedict, **kwargs)
                            else:
                                self.capture_image_url(image_num, imagedict, **kwargs)

                    elif (
                        pattern.search(kwargs["Section"]) is None
                    ) and section != "Alternates":
                        continue

                    else:
                        # The category of the image is unknown. Save and categorize it later
                        self.default_image_capture(image_num, imagedict, **kwargs)

                except IndexError:
                    self.default_image_capture(image_num, imagedict, **kwargs)

    @staticmethod
    def calculate_document_completeness(document):
        """Return completeness and image scores for survivor selection.

        Bookkeeping and aggregation-only fields do not contribute to the
        score. Missing values, ``None``, empty strings, and empty containers
        are not meaningful. Nested dictionary and list values are evaluated
        recursively so richer image and geodata structures score higher.

        Args:
            document: MongoDB property document to evaluate.

        Returns:
            A tuple containing the meaningful-value score and image score.
        """

        ignored_fields = {
            "_id", "Update_Log", "_cleanup_mlsnum", "_cleanup_mlsnum_type"}

        def meaningful_values(value):
            """
            Return a meaningful-value score for the given value. Recursively
            implements the function for nested values

            :param value: Value to evaluate.
            :return: Meaningful-value score.
            """
            if value is None or value == "":
                return 0
            if isinstance(value, dict):
                return sum(meaningful_values(item) for item in value.values())
            if isinstance(value, (list, tuple, set)):
                return sum(meaningful_values(item) for item in value)
            return 1

        completeness = sum(
            meaningful_values(value)
            for field, value in document.items()
            if field not in ignored_fields
        )
        image_score = meaningful_values(document.get("Images"))
        return completeness, image_score

    def capture_image_url(self, image_num, imagedict, **kwargs):

        filename = os.path.join(
            self.image_dir,
            kwargs["Section_Type"],
            kwargs["Condition"],
            kwargs["Address"] + " - " + kwargs["Section_Type"] + f"_{image_num}.png",
        )
        imagedict[kwargs["Section_Type"]].append(
            {"Condition": kwargs["Condition"], "URL": kwargs["image_url"], "Directory": filename}
        )

    def capture_front_image_url(self, image_num, imagedict, **kwargs):

        try:
            filename = os.path.join(
                self.image_dir,
                kwargs["Prop_Style"],
                kwargs["Condition"],
                kwargs["Address"] + " - " + "Front" + f"_{image_num}.png")

        except TypeError:
            filename = os.path.join(
                self.image_dir,
                "Front",
                kwargs["Condition"],
                kwargs["Address"] + " - " + "Front" + f"_{image_num}.png",
            )

        imagedict["Front"].append(
            {"Condition": kwargs["Condition"], "URL": kwargs["image_url"], "Directory": filename}
        )

    def check_for_database(self):

        if self.db_name in self.mongo_db_conn.list_database_names():
            print(f" ==== CURSER CONNECTED TO {self.db_name} DATABASE ==== ")

        else:
            print(
                f"THE {self.db_name} DATABASE PREVIOUSLY DID NOT EXIST, BUT HAS BEEN CREATED ==== "
            )

        return self.mongo_db_conn[self.db_name]

    @staticmethod
    def check_for_directory(directory):

        if os.path.exists(directory):
            pass
        else:
            os.makedirs(directory)
            print(f" ==== NEW DIRECTORY CREATED: {directory} ==== ")

    def check_for_collection(self):

        if self.col_name in self.database.list_collection_names():
            print(f" ==== THE {self.col_name} COLLECTION EXISTS ==== ")

        else:
            print(
                f" ==== THE {self.col_name} COLLECTION PREVIOUSLY DID NOT EXIST, BUT HAS BEEN CREATED ==== "
            )

        return self.database[self.col_name]

    @staticmethod
    def clean_image_key(property_data):

        raw_data = property_data["Images"].copy()

        for section, result_list in raw_data.items():
            if len(result_list) == 0:
                del property_data["Images"][section]
                # print(f' === {section.upper()} LIST EMPTY. DELETING KEY ==== ')

        return property_data

    def collect_image_data(self, target_row, property_data, **kwargs):

        if isinstance(target_row["IMAGES"], str):
            image_dict = self.create_image_dict()
            image_list = kwargs["image_pattern"].findall(target_row["IMAGES"])

            for image_num, image in enumerate(image_list):

                kwargs["Section"] = section = image[0].strip("'").split("-")[1].strip()
                kwargs["image_url"] = image[1].strip().strip("'")

                for section_type, pattern in self.home_sections.items():
                    kwargs["Section_Type"] = section_type
                    if pattern.search(section) is not None:
                        if section_type != "Alternates":

                            if section_type == "Front":

                                self.capture_front_image_url(image_num, image_dict, **kwargs)
                                break
                            else:
                                self.capture_image_url(image_num, image_dict, **kwargs)
                                break
                        else:
                            # Image of listing is the main image title and/or there's detail about the image
                            # in the subtext. Need to use a different method to capture the image name
                            self.alternates_image_capture(image_num, image_dict, **kwargs)

                    elif (pattern.search(section) is None) and section_type != "Alternates":
                        continue

                    else:
                        # The category of the image is unknown. Save and categorize it later
                        self.default_image_capture(image_num, image_dict, **kwargs)

            property_data["Images"] = image_dict
            RealEstateImages.clean_image_key(property_data)

    @staticmethod
    def create_agg_pipeline(include_non_integer=True):
        """Build the aggregation used to find canonical MLSNum groups.

        MLSNum values are converted to integers before grouping so values such
        as ``123`` and ``"123"`` are treated as the same listing. Every source
        document is returned with its group because survivor selection depends
        on ``Images_Downloaded`` and document completeness. Values that cannot
        be converted to an integer are excluded and handled separately by
        :meth:`invalid_mlsnum_documents`.

        Args:
            include_non_integer: Include single-document groups whose MLSNum
                requires datatype normalization in addition to duplicates.

        Returns:
            A MongoDB aggregation pipeline sorted by canonical MLSNum.
        """

        result_filter = [{"document_count": {"$gt": 1}}]
        if include_non_integer:
            result_filter.append({"requires_normalization": 1})

        return [
            {
                "$set": {
                    "_cleanup_mlsnum": {
                        "$convert": {
                            "input": "$MLSNum",
                            "to": "long",
                            "onError": None,
                            "onNull": None,
                        }
                    },
                    "_cleanup_mlsnum_type": {"$type": "$MLSNum"},
                }
            },
            {"$match": {"_cleanup_mlsnum": {"$ne": None}}},
            {
                "$group": {
                    "_id": "$_cleanup_mlsnum",
                    "documents": {"$push": "$$ROOT"},
                    "document_count": {"$sum": 1},
                    "requires_normalization": {
                        "$max": {
                            "$cond": [
                                {"$in": ["$_cleanup_mlsnum_type", ["int", "long"]]},
                                0,
                                1,
                            ]
                        }
                    },
                }
            },
            {"$match": {"$or": result_filter}},
            {"$sort": {"_id": 1}},
        ]

    @staticmethod
    def create_base_document(target_row, **kwargs):

        replace_pattern = re.compile("\.?\(\d{4}\)\*?")
        property_data = defaultdict(str)

        address = " ".join([str(target_row["STREETNUMDISPLAY"]), str(target_row["STREETNAME"]).upper()])
        target_date, condition = RealEstateImages.date_and_condition(target_row)
        date_str = target_date.split("T")[0]
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        new_town = re.sub(replace_pattern, "", str(target_row["TOWN"])).upper()
        prop_type, prop_style_type = RealEstateImages.property_style(target_row, property_data)

        kwargs["Address"] = property_data["Address"] = address.title()
        kwargs["MLSNum"] = property_data["MLSNum"] = int(target_row["MLSNUM"])
        kwargs["State"] = property_data["State"] = "NJ"
        kwargs["ListDate"] = property_data["Date"] = target_date
        kwargs["Condition"] = property_data["Condition"] = condition.title()
        kwargs["Town"] = property_data["Town"] = new_town.title()
        kwargs["Prop_Style"] = property_data["Prop_Style"] = prop_style_type
        kwargs["Zipcode"] = property_data["Zipcode"] = target_row["ZIPCODE"]
        kwargs["CountyCode"] = property_data["CountyCode"] = target_row["COUNTYCODE"]
        kwargs["BlockID"] = property_data["BlockID"] = target_row["BLOCKID"]
        kwargs["LotID"] = property_data["LotID"] = target_row["LOTID"]
        property_data["Update_Log"] = {
            "events":[
                {"operation":"document_creation",
                 "event_date":datetime.now()
                 }
            ]
        }

        try:
            if prop_type != 'RNT':
                kwargs["SalesPrice"] = property_data["Sales_Price"] = int(target_row["SALESPRICE"])
            else:
                kwargs["RentPrice"] = property_data["Rental_Price"] = int(target_row["RENTMONTHPERLSE"])
        except KeyError:
            pass

        try:
            property_data["Listing_Remarks"] = target_row["LISTING_REMARKS"]
            property_data["Geo_Data"] = {'Latitude': float(target_row["LATITUDE"]),
                                         'Longitude': float(target_row["LONGITUDE"])}
        except ValueError:
            pass

        return property_data, kwargs

    @staticmethod
    def create_futures_session():

        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=4,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"GET"},
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=8,
            pool_maxsize=8,
            pool_block=True,
        )

        base_session = Session()
        base_session.mount("http://", adapter)
        base_session.mount("https://", adapter)

        session = FuturesSession(
            session=base_session,
            max_workers=5
        )

        return session

    def create_image_dict(self):

        imagedict = {}
        image_sections_list = list(self.home_sections.keys())
        image_sections_list.append("Other")

        for section in image_sections_list:
            imagedict.setdefault(section, [])

        return imagedict

    @staticmethod
    def create_image_list(image_dict: dict):

        total_image_list = []

        for category in image_dict.keys():

            if image_dict[category] == []:
                continue
            else:
                total_image_list.extend(image_dict[category])

        return total_image_list

    @staticmethod
    def create_new_filename(filepath, mlsnum):

        filepath_list = filepath.split('/')

        if filepath_list[1] != 'raw':

            filepath_list = filepath_list[-3:]
            file_address = str(mlsnum) + " - " + filepath_list[-1]
            section = filepath_list[0]
            condition = filepath_list[1]

            return os.path.join('raw', 'images', 'original', section, condition, file_address)
        else:
            return filepath

    @staticmethod
    def create_update_log(existing_log, changes, operation="database_cleanup"):
        """Create or append to an ``Update_Log`` event history.

        Args:
            existing_log: Current Update_Log value. A dictionary containing an
                events list is preserved; a missing or malformed value starts
                a new history.
            changes: Dictionary describing changes made during this operation.
            operation: Name of the process responsible for the changes.

        Returns:
            A dictionary containing the existing history and a new event.
        """

        if isinstance(existing_log, dict) and isinstance(existing_log.get("events"), list):
            update_log = dict(existing_log)
            events = list(existing_log["events"])
        else:
            update_log = {}
            events = []

        events.append({
            "operation": operation,
            "event_date": pendulum.now(tz="UTC"),
            "changes": changes,
        })
        update_log["events"] = events
        return update_log

    @staticmethod
    def date_and_condition(series):

        try:

            prop_class = series["PROP_CLASS"]

            if prop_class == "RNT":
                date = series["RENTEDDATE"]

                if isinstance(date, float):
                    date = "0000-00-00"
            else:
                date = series["LISTDATE"]

                if isinstance(date, float):
                    date = "0000-00-00"

            condition = series["CONDITION"]

            return date, condition

        except KeyError:

            return "0000-00-00", "Unknown"

    def default_image_capture(self, image_num, imagedict, **kwargs):

        filename = os.path.join(
            self.image_dir,
            "Other",
            kwargs["Condition"],
            kwargs["Address"] + " - " + "Other" + f"_{image_num}.png",
        )

        imagedict["Other"].append(
            {"Condition": kwargs["Condition"], "URL": kwargs["image_url"], "Directory": filename}
        )

    def delete_duplicate_documents(self, duplicate_ids: list, logger):
        """Delete losing duplicate documents by exact MongoDB ``_id``.

        Args:
            duplicate_ids: Iterable of exact identifiers to remove.
            logger: Logger supplied by ``logger_decorator``.

        Returns:
            Number of documents deleted.

        Raises:
            RuntimeError: If MongoDB deletes fewer documents than requested.
        """

        duplicate_ids = list(duplicate_ids)
        if not duplicate_ids:
            return 0

        # PyMongo returns a DeleteResult object with a 'deleted_count' attribute
        result = self.collection.delete_many({"_id": {"$in": duplicate_ids}})
        if result.deleted_count != len(duplicate_ids):
            raise RuntimeError(
                f"Expected to delete {len(duplicate_ids)} duplicates but deleted "
                f"{result.deleted_count}"
            )
        logger.info(f"Deleted duplicate document ids: {duplicate_ids}")
        return result.deleted_count

    def does_document_exist(self, document_id):
        """Return True if the document with the given ID exists."""
        return self.collection.find_one({"_id": document_id}) is not None

    def invalid_mlsnum_documents(self):
        """Return documents whose MLSNum cannot be converted to an integer."""

        pipeline = [
            {
                "$set": {
                    "canonical_mlsnum": {
                        "$convert": {
                            "input": "$MLSNum",
                            "to": "long",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"canonical_mlsnum": None}},
            {"$project": {"_id": 1, "MLSNum": 1}},
        ]
        return list(self.collection.aggregate(pipeline))

    def fetch_mlsnums(self, batch_size):
        """
        Returns a list of MLSNum values
        """
        while True:
            last_mls = RealEstateImages.get_latest_mlsnum("gsmls_download_images", "last_mls")
            match = {"Images_Downloaded": {"$exists": False}}

            if last_mls is not None:
                print(f' ==== STARTING IMAGE DOWNLOAD FROM MLSNUM {last_mls} ==== ')
                match["MLSNum"] = {"$gt": last_mls}

            pipeline = [
                {"$match": match},
                {"$sort": {"MLSNum": 1}},
                {"$limit": batch_size},
            ]

            print(f' ==== GENERATING DOCUMENT BATCH OF SIZE {batch_size} FOR IMAGE DOWNLOADS ==== ')
            yield list(self.collection.aggregate(
                pipeline,
                batchSize=batch_size,
                allowDiskUse=True))

    def generate_current_isps(self, current_proxies):

        isps = {}
        raw_proxies_list = []
        default_ports = current_proxies[0]['proxy_data']['ports']

        for proxy_data in current_proxies:
            raw_proxies_list.extend(proxy_data['proxy_data']['proxies'])

        for idx, isp in enumerate(raw_proxies_list):
            isps[idx] = {"proxy": f"{isp['ip']}:{default_ports['http|https']}",
                         "proxy_auth": f"{isp['username']}:{isp['password']}"}

        # pprint(isps)
        self.isp_ips = isps
        print(" ==== TESTING PROXIES ==== ")
        try:
            self.test_proxies()
        except HTTPError:
            print(f" ==== HTTPBIN SERVICE UNAVAIALABLE. RETRYING PROXY TEST LATER === ")

    def generate_image_docs(self, batch_size=60):

        for image_batch in self.fetch_mlsnums(batch_size):

            for image_doc in image_batch:
                mls_num = image_doc["MLSNum"]
                yield image_doc
                check_pipeline_metadata("gsmls_download_images", prop_type_=None,
                                        key_="last_mls", status_=mls_num)

    def generate_proxy(self):

        if datetime.now() >= self.proxy_check_time + timedelta(minutes=10):
            print(" ==== TESTING PROXIES ==== ")
            self.proxy_check_time = datetime.now()
            try:
                self.test_proxies()
            except HTTPError:
                print(f" ==== HTTPBIN SERVICE UNAVAIALABLE. RETRYING PROXY TEST LATER === ")

        idx = random.randint(0, 19)
        proxy = self.isp_ips[idx]["proxy"]
        proxy_auth = self.isp_ips[idx]["proxy_auth"]
        # if idx in self.dead_ips:  I need to properly account for when dead_ips are chosen and how to rectify it

        proxies = {
            "http": f"http://{proxy_auth}@{proxy}",
            "https": f"http://{proxy_auth}@{proxy}",
        }

        return proxies

    @staticmethod
    def get_latest_mlsnum(pipeline, key):

        data_path = get_filepath("metadata")
        metadata_path = os.path.join(data_path, "metadata")

        try:
            with shelve.open(metadata_path) as reader:
                result = reader[pipeline]

            return result[key]
        except KeyError:
            check_pipeline_metadata(pipeline, prop_type_=None, key_=key)
            with shelve.open(metadata_path) as reader:
                result = reader[pipeline]

            return result[key]

    def get_residential_hash(self):

        # Obtain the residential user hash to conduct actions in IPRoyal
        url = 'https://resi-api.iproyal.com/v1/me'
        headers = {'Authorization': f'Bearer {self.ip_api}'}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            data = response.json()
            return data

    def get_static_proxy_order(self):

        data_isp_list = []
        headers_isp = {'X-Access-Token': f'{self.ip_api}', 'Content-Type': 'application/json'}

        for order_num in self.latest_isp_order_num:
            url_isp = f'https://apid.iproyal.com/v1/reseller/orders/{order_num}'
            response_isp = requests.get(url_isp, headers=headers_isp)

            if response_isp.status_code == 200:
                print(' ==== PREVIOUS ORDERS FOR ISP PROXIES ==== ')
                data_isp = response_isp.json()
                data_isp_list.append(data_isp)

        return data_isp_list

    @staticmethod
    def get_us_pw(website):
        """

        :param website:
        :return:
        """
        # Saves the current directory in a variable in order to switch back to it once the program ends
        previous_wd = os.getcwd()
        os.chdir("F:\\Jibreel Hameed\\Kryptonite")

        db = pd.read_excel("get_us_pw.xlsx", index_col=0)
        username = db.loc[website, "Username"]
        pw = db.loc[website, "Password"]
        base_url = db.loc[website, "Base URL"]

        os.chdir(previous_wd)

        return username, base_url, pw

    @staticmethod
    def load_ip_api():

        filepath = get_filepath('env')
        load_dotenv(filepath)

        return os.getenv('IPROYAL_API')

    def max_mlsnum(self):

        results = self.collection.find({}, {"MLSNum": 1}).sort("MLSNum", -1).limit(1)

        for result in results:
            return result['MLSNum']

    @staticmethod
    def parse_request_error(error, file_data, **kwargs):

        base_url = "https://img.gsmls.com"
        error_url_pattern = re.compile(r'url: (.*.jpg)')
        error_type = type(error).__name__
        image_url = error_url_pattern.search(str(error)).group(1)
        full_url = base_url + image_url
        idx = file_data['url'].index(full_url)
        suspected_proxy = file_data['proxy'][idx]['pycharm_display_http']

        kwargs['logger'].warning(f' ==== FUTURES REQUEST ERROR! ERROR TYPE: {error_type} @ {suspected_proxy}')

        return full_url

    @staticmethod
    def prepare_data(image_list):

        batch_size = 10
        for i in range(0, len(image_list), batch_size):
            yield image_list[i:i + batch_size]

    @staticmethod
    def property_style(series, prop_data):
        """
        REFACTOR
        """

        try:
            if series["STYLEPRIMARY_SHORT"]:
                if isinstance(series["STYLEPRIMARY_SHORT"], float):
                    res_style = np.nan

                elif series["STYLEPRIMARY_SHORT"] == "SeeRem":
                    res_style = np.nan

                else:
                    res_style = series["STYLEPRIMARY_SHORT"]

        except KeyError:
            res_style = np.nan

        try:
            if series["UNITSTYLE_SHORT"]:
                if isinstance(series["UNITSTYLE_SHORT"], float):
                    mul_style = np.nan

                elif series["UNITSTYLE_SHORT"] == "SeeRem":
                    mul_style = np.nan

                else:
                    mul_style = series["UNITSTYLE_SHORT"]

        except KeyError:
            mul_style = np.nan

        try:
            if series["PROPSUBTYPERN"]:
                if isinstance(series["PROPSUBTYPERN"], float):
                    rnt_style = np.nan

                else:
                    rnt_style = series["PROPSUBTYPERN"]

        except KeyError:
            rnt_style = np.nan

        if (
            isinstance(res_style, float)
            and isinstance(mul_style, float)
            and isinstance(rnt_style, float)
        ):
            return None, None
        elif not isinstance(res_style, float):
            return "RES", RealEstateImages.style_type_split(res_style, prop_data)
        elif not isinstance(mul_style, float):
            return "MUL", RealEstateImages.style_type_split(mul_style, prop_data)
        elif not isinstance(rnt_style, float):
            return "RNT", RealEstateImages.style_type_split(rnt_style, prop_data)

    def prepare_image_for_aws(self, futures_list, file_data, session, **kwargs):

        error_url_pattern = re.compile(r'url: (.*.jpg)')
        retriable_errors = (ProtocolError, IncompleteRead,
                            ChunkedEncodingError, ConnectionError,
                            SSLError, ProxyError)

        for _, future in zip(tqdm(range(kwargs['total_images']), desc='Images', file=sys.stderr,
                                  dynamic_ncols=True), as_completed(futures_list)):

            try:
                response = future.result()

            except retriable_errors as e:
                image_url = RealEstateImages.parse_request_error(e, file_data, **kwargs)
                idx = file_data['url'].index(image_url)
                filepath = file_data['directory'][idx]
                response = self.single_session_request(session, image_url, retriable_errors, **kwargs)
                if response is not None:
                    RealEstateImages.store_image_in_aws(response, filepath, **kwargs)

            except ClientError as e:
                base_url = error_url_pattern.search(str(e)).group(1)
                kwargs['logger'].warning(f'{e}')
                kwargs['logger'].warning(f' ==== IMAGE DID NOT UPLOAD TO AWS S3 ==== ')
                kwargs['logger'].warning(f"MLSNUM: {kwargs['metadata']['mlsnum']} ===== URL: {base_url} ===== ")
            else:
                url = response.url
                idx = file_data['url'].index(url)
                filepath = file_data['directory'][idx]

                RealEstateImages.store_image_in_aws(response, filepath, **kwargs)

    def proxy_manager(self):

        # Obtain the residential user hash to conduct actions in IPRoyal
        user_data = self.get_residential_hash()
        isp_orders = self.get_static_proxy_order()
        isp_expiration = pendulum.parse(isp_orders[-1]['expire_date'])
        print(f' ==== CURRENT PROXY EXPIRATION DATE: {isp_expiration} ==== ')
        available_traffic = float(user_data['available_traffic'])
        print(f' ==== CURRENT  RESIDENTIAL PROXY AVAILABLE TRAFFIC: {available_traffic} ==== ')

        # Only purchase data if the latest proxy purchase is 30 days old and there's less than 1.5GB
        # of available traffic left
        if available_traffic < 2:
            print(f' ==== RESIDENTIAL PROXY DATA HAS REACHED ITS LOWER LIMIT. DETERMINING DATA INCREASE ==== ')

        if isp_expiration < pendulum.now(tz=timezone("America/New_York")):
            print(f' ==== MORE STATIC PROXY DATA NEEDS TO BE PURCHASED ==== ')
            # try:
            #     purchasing_data()
            # except some_error:
            #     print(f' ==== DATA PURCHASE UNSUCCESSFUL ==== ')
            #     self.static_ip_status = "Expired"

        elif isp_expiration > pendulum.now(tz=timezone("America/New_York")):
            print(f' ==== STATIC PROXIES ARE STILL ACTIVE ==== ')

        self.generate_current_isps(isp_orders)
        # self.generate_current_res_ip()

    def request_image(self, session, image_list: list, **kwargs):

        total_images = 0
        mlsnum = kwargs['metadata']['mlsnum']
        futures = []
        files_data = {
            'url': [],
            'directory': [],
            'proxy': []
        }

        for batch in RealEstateImages.prepare_data(image_list):
            for image in batch:
                url = image["URL"]
                proxy = self.generate_proxy()
                file_directory = RealEstateImages.create_new_filename(image["Directory"], mlsnum)
                files_data['url'].append(url)
                files_data['directory'].append(file_directory)
                files_data['proxy'].append(proxy)
                future = session.get(url, proxies=proxy)
                futures.append(future)
                total_images += 1
                self.total_images += 1

        print(f" ==== STORING {total_images} IMAGES FOR {kwargs['metadata']['mlsnum']} - {kwargs['metadata']['address']} ==== ")
        kwargs['total_images'] = total_images
        self.prepare_image_for_aws(futures, files_data, session, **kwargs)

    @staticmethod
    def select_duplicate_survivor(documents):
        """Select a duplicate survivor without merging losing documents.

        Presence of ``Images_Downloaded`` has first priority. Completeness and
        image scores resolve ties, followed by the string form of ``_id`` for
        deterministic behavior.

        Args:
            documents: Documents belonging to one canonical MLSNum group.

        Returns:
            A tuple of the selected survivor and the losing documents.

        Raises:
            ValueError: If no documents are supplied.
        """

        def survivor_score(document):
            """
            Return a survivor score for the given document.
            Example output of survivor score:
            document_a = (1, 12, 5, "...")
            document_b = (0, 20, 9, "...")

            :param document: Document to evaluate.
            :return: Survivor score.
            """
            completeness, image_score = RealEstateImages.calculate_document_completeness(document)
            return (
                int("Images_Downloaded" in document),  # Boolean determinant to 1 or 0
                completeness,
                image_score,
                str(document["_id"]),
            )

        if not documents:
            raise ValueError("Cannot select a survivor from an empty document list")

        # Selects the document with the highest score starting from left most index
        survivor = max(documents, key=survivor_score)
        losing_documents = [doc for doc in documents if doc["_id"] != survivor["_id"]]
        return survivor, losing_documents

    def single_session_request(self, session, url, error_list, **kwargs):

        attemps = 0
        max_retries = 5
        error_base = ' ==== SINGLE SESSION REQUEST ERROR! ERROR TYPE:'

        while attemps < max_retries:
            proxy = self.generate_proxy()
            future = session.get(url, proxies=proxy)

            try:
                response = future.result()
                if response.status_code == 200:
                    return response

            except error_list as e:
                error_type = type(e).__name__
                kwargs['logger'].warning(f'{error_base} {error_type} @ {proxy["pycharm_display_http"]}')
                attemps += 1

        kwargs['logger'].warning(f' ==== SINGLE SESSION REQUEST ERROR MAX ATTEMPTS REACHED. {url} NOT DOWNLOADED ==== ')
        return None

    @staticmethod
    def sleep_variation(image_num: int):

        random_num = random.randint(1, 25)

        if image_num > random_num:

            # print(f'Long wait: {random.uniform(0.8, 3.7)}')
            time.sleep(random.uniform(1.8, 5.7))

        else:
            # print(f'Short wait: {random.uniform(0.8, 1.7)}')
            time.sleep(random.uniform(1.8, 3.7))

    def sql_query(self, series):

        prop_type = {
            "RES": "res_properties",
            "MUL": "mul_properties",
            "RNT": "rnt_properties",
            "LND": "lnd_properties"
        }

        try:

            prop_class = series["PROP_CLASS"]
            mls_num = series["MLSNUM"]

            if prop_class == "RNT":
                date = series["RENTEDDATE"]
            else:
                date = series["LISTDATE"]

            query = (
                f"SELECT * FROM {prop_type[prop_class]} WHERE \"MLSNUM\" = '{mls_num}';"
            )
            data = pd.read_sql_query(query, con=self.sql_conn)
            condition = data["CONDITION"].values[0]

            return date, condition

        except KeyError:

            return "0000-00-00", "Unknown"

    @staticmethod
    def store_image_in_aws(response, filepath, **kwargs):

        if response.status_code == 200:
            image_data = response.content
            kwargs['s3_client'].upload_fileobj(BytesIO(image_data), "amzn-s3-gsmls-propertyimages",
                                               filepath, ExtraArgs={'Metadata': kwargs['metadata']})

    @staticmethod
    def style_type_split(style_type, prop_data):
        """
        REFACTOR
        """

        if (style_type is not None) and ("," in style_type):
            style_type_list = style_type.split(",")
            if "Duplex" in style_type_list:

                return "Duplex"

            elif "Triplex" in style_type_list:

                return "Triplex"

            elif "FourPlex" in style_type_list:

                return "FourPlex"

            elif (style_type_list[0] or style_type_list[1]) in [
                "Cluster",
                "UndrOver",
                "TwoStory",
                "ThreStry",
                "OneStory",
            ]:
                if "FixrUppr" in style_type_list:
                    prop_data["Condition"] = "FIXER UPPER"

                return "MultiFam"

        elif style_type in ["Cluster", "UndrOver", "TwoStory", "ThreStry", "OneStory"]:

            return "MultiFam"

        elif style_type == "Resident":

            return "Residential"

        elif style_type == "SeeRem":

            return None

        elif style_type == "FixrUppr":

            prop_data["Condition"] = "FIXER UPPER"
            return None

        else:

            return style_type

    def test_proxies(self):

        for key, value in self.isp_ips.items():

            proxy = value["proxy"]
            proxy_auth = value["proxy_auth"]

            proxies = {
                "http": f"http://{proxy_auth}@{proxy}",
                "https": f"http://{proxy_auth}@{proxy}",
            }

            try:
                response = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=20)
                if response.status_code == 200:
                    if response.json()['origin'] != proxy.split(':')[0]:
                        if key not in self.dead_ips:
                            self.dead_ips.append(key)
                else:
                    response.raise_for_status()

            except HTTPError as e:
                if e.response.status_code in [503, 504]:
                    e.response.raise_for_status()
                else:
                    print(
                        f" ==== UNKNOWN HTTPERROR FOR PROXY http://{proxy_auth}@{proxy} "
                        f"DURING TEST. STATUS CODE {e.response.status_code} ==== ")
                    if key not in self.dead_ips:
                        self.dead_ips.append(key)
            except ProxyError as e:
                print(f" ==== PROXYERROR FOR http://{proxy_auth}@{proxy} "
                      f"DURING TEST. STATUS CODE {e.response.status_code} ==== ")
                if key not in self.dead_ips:
                    self.dead_ips.append(key)
            except requests.exceptions.Timeout:
                print(f" ==== PROXY http://{proxy_auth}@{proxy} TIMED OUT DURING TEST ==== ")
                if key not in self.dead_ips:
                    self.dead_ips.append(key)

        print(f" ==== DEAD IPS HAVE BEEN CAPTURED. RESUMING IMAGE DOWNLOADS ==== ")

    @staticmethod
    def update_date_datatype(date_value, update_op):

        if isinstance(date_value, float):
            # Date value is nan
            update_op["$set"].update(
                {"Date": datetime.strptime("1970-12-31", "%Y-%m-%d")}
            )
        elif isinstance(date_value, str):
            # Date is unknown
            if date_value == "0000-00-00":
                update_op["$set"].update(
                    {"Date": datetime.strptime("1970-12-31", "%Y-%m-%d")}
                )
            elif "/" in date_value:
                update_op["$set"].update(
                    {"Date": datetime.strptime(date_value, "%m/%d/%Y %H:%M:%S")}
                )
            elif "-" in date_value:
                date_str = date_value.split("T")[0]
                update_op["$set"].update(
                    {"Date": datetime.strptime(date_str, "%Y-%m-%d")}
                )

    def update_geodata(self, mlsnum, field_val, update_op):

        if field_val is None:

            query = f"SELECT latitude, longitude FROM gsmls_imputed_data WHERE mlsnum = '{mlsnum}';"

            data = pd.read_sql(query, self.sql_conn).squeeze()

            if data.empty is False:
                if data["latitude"] == "0E-20" and data["longitude"] == "0E-20":
                    update_op["$set"].update(
                        {"Geo_Data": {"Latitude": None, "Longitude": None}}
                    )
                else:
                    update_op["$set"].update(
                        {
                            "Geo_Data": {
                                "Latitude": data["latitude"],
                                "Longitude": data["longitude"],
                            }
                        }
                    )

    @staticmethod
    def update_image_object(image_obj, update_op):
        """Schedule removal of empty image categories from a document."""

        if not isinstance(image_obj, dict):
            return

        for category, value in image_obj.items():
            if len(value) == 0:
                update_op["$unset"].update({f"Images.{category}": ""})

    @staticmethod
    def update_str_values(town_val, address_val, condition_val, zip_val, update_op):
        """Schedule safe casing and ZIP code normalization operations."""

        if isinstance(town_val, str) and town_val == town_val.upper():
            update_op["$set"].update({"Town": town_val.title()})

        if isinstance(address_val, str) and address_val == address_val.upper():
            update_op["$set"].update({"Address": address_val.title()})

        if isinstance(condition_val, str) and condition_val == condition_val.upper():
            update_op["$set"].update({"Condition": condition_val.title()})

        if isinstance(zip_val, float):
            pass

        elif isinstance(zip_val, int):
            pass

        elif isinstance(zip_val, str) and len(zip_val) == 4:
            update_op["$set"].update({"Zipcode": "0" + zip_val})

    """
    ----------------------------------------------------------------------------------------------------------------
                                                MAJOR FUNCTIONS
    ----------------------------------------------------------------------------------------------------------------
    """

    @logger_decorator
    def database_cleanup(self, cutoff_time, **kwargs):
        """Normalize property documents and remove canonical MLSNum duplicates.

        Duplicate groups are built after converting MLSNum values to integers.
        A document containing ``Images_Downloaded`` has survivor priority;
        completeness and image content resolve ties. Losing documents are not
        merged and are deleted by exact ``_id``. The survivor then receives the
        existing datatype, date, geodata, casing, and image cleanup operations.
        Every applied change is appended to its ``Update_Log`` dictionary.

        Args:
            cutoff_time: Zoned datetime after which cleanup must stop.
            **kwargs: Receives the logger injected by ``logger_decorator``.

        Returns:
            ``True`` only when no duplicate or non-integer convertible MLSNum
            groups remain and no invalid MLSNum values were found; otherwise
            ``False``.
        """

        logger = kwargs["logger"]
        logger.info(f" ==== CURRENT DOCUMENT COUNT FOR {self.db_name}.{self.col_name} ==== \n"
                    f" ==== TOTAL: {self.collection.count_documents({})}")

        try:
            # Be sure to handle invalid MLSNum values. Deletion should be the default behavior.
            invalid_documents = self.invalid_mlsnum_documents()
            if invalid_documents:
                logger.error(
                    f" ==== MLSNUM VALUES COULD NOT BE CONVERTED TO INTEGERS: {invalid_documents} ==== "
                )

            print(' ==== GATHERING DOCUMENTS FROM AGGREGATE PIPELINE ==== ')
            duplicate_cursor = self.collection.aggregate(
                RealEstateImages.create_agg_pipeline(),
                allowDiskUse=True,
                batchSize=100,
            )
            for result in duplicate_cursor:
                assert pendulum.now(tz=timezone("America/New_York")) < cutoff_time, \
                    f" ==== DATABASE CLEANING CUTOFF TIME HAS BEEN REACHED ==== "
                canonical_mlsnum = int(result["_id"])  # Canonical grouped MLSNum
                survivor, losing_documents = RealEstateImages.select_duplicate_survivor(
                    result["documents"]
                )
                losing_ids = [document["_id"] for document in losing_documents]  # List of MongoDB document identifiers
                logger.info(f" ==== CURRENT DOCUMENT: {canonical_mlsnum} ==== ")

                deleted_count = self.delete_duplicate_documents(losing_ids, logger)
                update_operation = {"$set": {}, "$unset": {}}
                changes = {}

                original_mlsnum = survivor.get("MLSNum")
                if type(original_mlsnum) is not int or original_mlsnum != canonical_mlsnum:
                    update_operation["$set"]["MLSNum"] = canonical_mlsnum
                    changes["MLSNum"] = {
                        "from": original_mlsnum,
                        "to": canonical_mlsnum,
                    }

                self.update_geodata(
                    canonical_mlsnum,
                    survivor.get("Geo_Data"),
                    update_operation,
                )
                RealEstateImages.update_date_datatype(
                    survivor.get("Date"),
                    update_operation,
                )
                RealEstateImages.update_str_values(
                    survivor.get("Town", ""),
                    survivor.get("Address", ""),
                    survivor.get("Condition", ""),
                    survivor.get("Zipcode"),
                    update_operation,
                )
                RealEstateImages.update_image_object(
                    survivor.get("Images", {}),
                    update_operation,
                )
                if "Image_Downloaded" in survivor:
                    update_operation["$unset"]["Image_Downloaded"] = ""

                for field, new_value in list(update_operation["$set"].items()):
                    old_value = survivor.get(field)
                    if old_value == new_value:
                        del update_operation["$set"][field]
                    elif field not in changes:
                        changes[field] = {"from": old_value, "to": new_value}

                for field in update_operation["$unset"]:
                    if field.startswith("Images."):
                        category = field.split(".", 1)[1]
                        old_value = survivor.get("Images", {}).get(category)
                    else:
                        old_value = survivor.get(field)
                    changes.setdefault("fields_removed", []).append({
                        "field": field,
                        "from": old_value,
                        "to": "removed",
                    })

                if deleted_count:
                    changes["duplicates_removed"] = {
                        "count": deleted_count,
                        "removed_ids": losing_ids,
                    }

                existing_update_log = survivor.get("Update_Log")
                if (
                    isinstance(existing_update_log, dict)
                    and isinstance(existing_update_log.get("events"), list)
                ):
                    # Preserve document_creation and append database_cleanup.
                    updated_log = RealEstateImages.create_update_log(
                        existing_update_log,
                        changes,
                    )
                else:
                    # Older documents begin their history with database_cleanup.
                    updated_log = RealEstateImages.create_update_log(None, changes)

                update_operation["$set"]["Update_Log"] = updated_log
                update_operation = {
                    operator: values
                    for operator, values in update_operation.items()
                    if values
                }
                update_result = self.collection.update_one(
                    {"_id": survivor["_id"]},
                    update_operation,
                )
                if update_result.matched_count != 1:
                    raise RuntimeError(
                        f"Survivor {survivor['_id']} was not available for update"
                    )

                check_pipeline_metadata(
                    "gsmls_cleaning_pipeline",
                    prop_type_=None,
                    key_="start_mls",
                    status_=canonical_mlsnum,
                )

            remaining_results = list(self.collection.aggregate(
                RealEstateImages.create_agg_pipeline(),
                allowDiskUse=True,
            ))
            if remaining_results:
                logger.error(
                    f"Database cleanup verification failed. Remaining groups: "
                    f"{[result['_id'] for result in remaining_results]}"
                )
                return False

        except CursorNotFound as cnf:
            logger.warning(f"{cnf}")
            logger.info("Aggregate cursor expired before cleanup completed")
            return False
        except AssertionError as e:
            logger.info(f"{e}")
            logger.info(f" ==== DATABASE CLEANING COMPLETED ==== ")
            return False
        else:
            logger.info(f" ==== DATABASE CLEANING COMPLETED ==== ")
            return True

    @logger_decorator
    def download_images_main(self, cutoff_time, **kwargs):
        """
        Queries each document and downloads the images stored in the Images field

        :param cutoff_time:
        :param kwargs:
        :return:
        """

        outer_update_operation = {"$set": {"Images_Downloaded": "Yes"}}
        session = RealEstateImages.create_futures_session()
        kwargs['s3_client'] = boto3.client('s3')
        max_mls = self.max_mlsnum()

        try:
            assert self.static_ip_status == "active", (" ==== STATIC IPS HAVE EXPIRED. "
                                                   "PURCHASE MORE DATA TO DOWNLOAD IMAGES ==== ")
        except AssertionError as e:
            print(f'{e}')
            return "Expired"

        try:
            while True:
                for _, record in zip(tqdm(range(60), desc='Records', file=sys.stderr,
                                          dynamic_ncols=True), self.generate_image_docs()):

                    assert pendulum.now(tz=timezone("America/New_York")) < cutoff_time, \
                        f" ==== IMAGE DOWNLOAD CUTOFF TIME HAS BEEN REACHED ==== "

                    if record:
                        # Access the Images key in the main dictionary
                        image_dict = record["Images"]
                        query_filter = {"MLSNum": record["MLSNum"]}
                        kwargs['metadata'] = {
                            'address': str(record["Address"]),
                            'mlsnum': str(record["MLSNum"]),
                            'town': str(record["Town"]),
                            'prop_style': str(record["Prop_Style"]),
                            'condition': str(record['Condition'])
                            }

                        # Loop through all the image categories and access each image
                        image_list = RealEstateImages.create_image_list(image_dict)
                        self.request_image(session, image_list, **kwargs)
                        self.total_props += 1
                        # Function which introduces variability between the image requests
                        RealEstateImages.sleep_variation(len(image_list))

                        # If the key doesn't exist in the dictionary, create the field
                        if record.get("Images_Downloaded", None) is None:
                            self.collection.update_one(query_filter, outer_update_operation)

                latest_imgs_downloaded = current_status("gsmls_download_images", None, "last_mls")
                if latest_imgs_downloaded == max_mls:
                    print(f' ==== NO FURTHER IMAGES TO BE DOWNLOADED ==== ')
                    break

        except AssertionError as e:
            print(f"{e}")
            print(f" ==== PROGRAM COMPLETED ==== ")
            return False
        else:
            print(f" ==== PROGRAM COMPLETED ==== ")
            return True

    def main(self, df_var, **kwargs):
        """
        Stores real estate property image data from a Pandas dataframe
        :return:
        """

        image_pattern = re.compile(r"'([^']+?)'\s*:\s*'(https:\/\/img\.gsmls\.com\/imagedb\/highres\/[^']+?\.jpg)'")
        kwargs["image_pattern"] = image_pattern

        for _, row_data in zip(tqdm(range(len(df_var)), "Row"), df_var.iterrows()):

            target_row = row_data[1]

            try:
                if (
                    (target_row["IMAGES"] == "None")
                    or isinstance(target_row["IMAGES"], float)
                    or (image_pattern.findall(target_row["IMAGES"]) == [])
                ):
                    print(f" ==== NO DATA FOUND ==== ")
                    continue
            except TypeError:
                print(f" ==== TYPEERROR: NO DATA FOUND ==== ")
                continue

            property_data, kwargs = RealEstateImages.create_base_document(target_row, **kwargs)
            self.collect_image_data(target_row, property_data, **kwargs)

            if not self.does_document_exist(property_data["MLSNum"]):
                self.collection.insert_one(dict(property_data))
                print(f" ==== NEW PROPERTY DOCUMENT CREATED IN MONGODB: "
                      f"{property_data['MLSNum']} - {property_data['Address']}, {property_data['Town']} ==== ")

