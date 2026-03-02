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
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
from pymongo.errors import CursorNotFound
from botocore.exceptions import ClientError
from urllib3.exceptions import ProtocolError, IncompleteRead
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
    def create_agg_pipeline():

        pipeline = [
            # Match string OR date types
            {
                "$match": {
                    "Date": {
                        "$type": ["string", "date"]
                    }
                }
            },

            # Group by MLSNum
            {
                "$group": {
                    "_id": "$MLSNum",

                    "Date": {"$push": "$Date"},
                    "Address": {"$push": "$Address"},
                    "Town": {"$push": "$Town"},
                    "Zipcode": {"$push": "$Zipcode"},
                    "Condition": {"$push": "$Condition"},
                    "Images": {"$push": "$Images"},

                    # Push Geo_Data only if it exists
                    "Geo_Data": {
                        "$push": {
                            "$cond": [
                                {"$ne": ["$Geo_Data", None]},
                                "$Geo_Data",
                                "$$REMOVE"
                            ]
                        }
                    },

                    # Count documents per MLSNum
                    "document_count": {"$sum": 1},

                    # Preserve old _id
                    "property_attr": {
                        "$push": {
                            "old_id": "$_id"
                        }
                    },
                }
            },

            # Sort descending
            {
                "$sort": {"document_count": -1}
            }
        ]

        return pipeline

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

    def delete_duplicates(self, doc_count, id_num, logger):

        if int(doc_count) >= 2:

            # Log how many documents will be deleted
            print(
                f" ==== MLSNUM {id_num} HAS {doc_count} DUPLICATED DOCUMENTS STORED ==== "
                f"Program will delete {int(doc_count) - 1} documents"
            )

            for _ in range(int(doc_count) - 1):
                self.collection.delete_one({"MLSNum": id_num})

            logger.info(
                f'New document count for {id_num}: {self.collection.count_documents({"MLSNum": id_num})}'
            )

    def fetch_duplicate_mlsnums(self, batch_size=500):
        """
        Returns a list of MLSNum values >= start_mls.
        If start_mls is None, just return first batch_size documents.
        """

        while True:
            query = {}
            start_mls = RealEstateImages.get_latest_mlsnum("gsmls_cleaning_pipeline", "start_mls")

            if start_mls is not None:
                print(f" ==== DUPLICATE MLS QUERY WILL START FROM: {start_mls} ==== ")
                query["MLSNum"] = {"$gte": start_mls}

            cursor = self.collection.find(
                query,
                {"MLSNum": 1, "_id": 0}
            ).sort("MLSNum", 1).limit(batch_size)

            # Turns the Mongo query cursor into a list
            mls_list = [doc["MLSNum"] for doc in cursor]

            if len(mls_list) > 1:
                print(' ==== GENERATING LIST OF NEW DUPLICATE DOCUMENTS ==== ')
                yield mls_list
                # for mls_num in mls_list:
                #     yield mls_num
            else:
                # No further MLSNum to provide. Breaks while query
                RealEstateImages.no_more_results(mls_list[0])
                break

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

        for idx, isp in enumerate(current_proxies['proxies']):
            isps[idx] = {"proxy": f"{isp['ip']}:{current_proxies['ports']['http|https']}",
                         "proxy_auth": f"{isp['username']}:{isp['password']}"}

        # pprint(isps)
        self.isp_ips = isps
        print(" ==== TESTING PROXIES ==== ")
        self.test_proxies()

    def generate_duplicate_mlsnums(self, cutoff_time, batch_size=500):

        idx_checkpoint = 0

        for mls_list in self.fetch_duplicate_mlsnums(batch_size=batch_size):

            for mls_num in mls_list:

                assert pendulum.now(tz=timezone("America/New_York")) < cutoff_time, \
                    f" ==== IMAGE DOWNLOAD CUTOFF TIME HAS BEEN REACHED ==== "
                count = self.collection.count_documents({"MLSNum": mls_num})

                if count <= 1:
                    print(f' ==== NO DUPLICATES LOCATED FOR MLSNUM {mls_num} ==== ')

                    if idx_checkpoint >= batch_size:
                        # In order to reduce the amount of writes, save points occur when the checkpoint
                        # equals the batch size. Reset idx_checkpoint
                        idx_checkpoint = 0
                        print(f' ==== INDEX CHECKPOINT REACHED ==== ')
                        check_pipeline_metadata("gsmls_cleaning_pipeline",
                                                prop_type_=None, key_="start_mls", status_=mls_num)

                    idx_checkpoint += 1
                    continue

                # Fetch all documents for inspection / cleanup
                docs = list(self.collection.find({"MLSNum": mls_num}))

                # Yield or process: MLSNum, docs, count
                yield mls_num, docs, count
                idx_checkpoint += 1
                check_pipeline_metadata("gsmls_cleaning_pipeline",
                                        prop_type_=None, key_="start_mls", status_=mls_num)

    def generate_image_docs(self, batch_size=60):

        for image_batch in self.fetch_mlsnums(batch_size):

            for image_doc in image_batch:
                mls_num = image_doc["MLSNum"]
                yield image_doc
                check_pipeline_metadata("gsmls_download_images", prop_type_=None,
                                        key_="last_mls", status_=mls_num)

    def generate_proxy(self, logger=None):

        # Only use static proxies which have authentication to access https://img.gsmls.com
        proxy_dict = {
            'residential': {"proxy": "geo.iproyal.com:12321",
                            "proxy_auth": "EC0m7tQy2GtYN9nv:QgurSG8NEOo6TYE3"},
            'isp': self.isp_ips
        }

        if datetime.now() >= self.proxy_check_time + timedelta(minutes=10):
            print(" ==== TESTING PROXIES ==== ")
            self.proxy_check_time = datetime.now()
            self.test_proxies(logger)

        num = random.randint(1, 100)

        if num <= 25:
            proxy = proxy_dict['residential']["proxy"]
            proxy_auth = proxy_dict['residential']["proxy_auth"]

        else:
            idx = random.randint(0, 9)
            proxy = proxy_dict['isp'][idx]["proxy"]
            proxy_auth = proxy_dict['isp'][idx]["proxy_auth"]
            if idx in self.dead_ips:
                # proxy 1 and 2 failed during the test
                proxy = proxy_dict['residential']["proxy"]
                proxy_auth = proxy_dict['residential']["proxy_auth"]

        proxies = {
            "http": f"http://{proxy_auth}@{proxy}",
            "https": f"http://{proxy_auth}@{proxy}",
        }

        # print(f' ==== PROXY IN USE: {proxies} ==== ')
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

        url_isp = f'https://apid.iproyal.com/v1/reseller/orders/{self.latest_isp_order_num}'
        # url_isp = f'https://apid.iproyal.com/v1/reseller/orders'
        headers_isp = {'X-Access-Token': f'{self.ip_api}', 'Content-Type': 'application/json'}
        # params = {
        #     'product_id': 22,
        #     'page': 1,
        #     'per_page': 10,
        #     'status': 'confirmed',
        #     'order_ids': [64872924]
        # }

        response_isp = requests.get(url_isp, headers=headers_isp)

        if response_isp.status_code == 200:
            print(' ==== PREVIOUS ORDERS FOR ISP PROXIES ==== ')
            data_isp = response_isp.json()
            return data_isp

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

    @staticmethod
    def no_more_results(mls_num):

        if mls_num == current_status("gsmls_cleaning_pipeline", "start_mls"):
            # Reset the metadata key and end the program
            check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None, key_="start_mls")
            print(' ==== NO MORE DUPLICATION RESULTS AVAILABLE ==== ')
            print(' ==== RESETTING START_MLS KEY IN METADATA ==== ')

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

    def proxy_manager(self):

        # Obtain the residential user hash to conduct actions in IPRoyal
        user_data = self.get_residential_hash()
        isp_orders = self.get_static_proxy_order()
        isp_expiration = pendulum.parse(isp_orders['expire_date'])
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

        self.generate_current_isps(isp_orders['proxy_data'])
        # self.generate_current_res_ip()

    def request_image(self, session, image_list: list, **kwargs):

        total_images = 0
        mlsnum = kwargs['metadata']['mlsnum']
        futures = []
        files_data = {
            'url': [],
            'directory': []
        }

        for batch in RealEstateImages.prepare_data(image_list):
            for image in batch:
                url = image["URL"]
                file_directory = RealEstateImages.create_new_filename(image["Directory"], mlsnum)
                files_data['url'].append(url)
                files_data['directory'].append(file_directory)
                future = session.get(url, stream=True, proxies=self.generate_proxy(logger=kwargs['logger']))
                futures.append(future)
                total_images += 1
                self.total_images += 1

        print(f" ==== STORING {total_images} IMAGES FOR {kwargs['metadata']['mlsnum']} - {kwargs['metadata']['address']} ==== ")
        kwargs['total_images'] = total_images
        RealEstateImages.store_in_aws(futures, files_data, **kwargs)

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
    def store_in_aws(futures_list, file_data, **kwargs):

        max_retries = 5
        error_url_pattern = re.compile(r'url: (.*).jpg')
        retriable_errors = (ProtocolError, IncompleteRead,
                            ChunkedEncodingError, ConnectionError,
                            SSLError, ProxyError)

        for _, future in zip(tqdm(range(kwargs['total_images']), desc='Images', file=sys.stderr,
                                  dynamic_ncols=True), as_completed(futures_list)):
            for attempt in range(max_retries):
                try:
                    response = future.result()
                    url = response.url
                    idx = file_data['url'].index(url)
                    filepath = file_data['directory'][idx]

                    if response.status_code == 200:
                        response.raw.decode_content = True
                        kwargs['s3_client'].upload_fileobj(response.raw, "amzn-s3-gsmls-propertyimages",
                                                           filepath, ExtraArgs={'Metadata': kwargs['metadata']})
                except ClientError as e:
                    base_url = error_url_pattern.search(str(e)).group(1)
                    kwargs['logger'].warning(f'{e}')
                    kwargs['logger'].warning(f' ==== IMAGE DID NOT UPLOAD TO AWS S3 ==== ')
                    kwargs['logger'].warning(f"MLSNUM: {kwargs['metadata']['mlsnum']} ===== URL: {base_url} ===== ")
                except retriable_errors as e:
                    base_url = error_url_pattern.search(str(e)).group(1)
                    if attempt < max_retries:
                        sleep_time = 2 ** attempt
                        kwargs['logger'].warning(
                            f"MLSNUM: Error: {kwargs['metadata']['mlsnum']} ===== URL: {base_url} ===== ")
                        kwargs['logger'].warning(f' ==== SLEEPING FOR {sleep_time} SECS THEN RETRYING ==== ')
                        time.sleep(sleep_time)
                        continue
                    kwargs['logger'].warning(f' ==== MAX TRIES REACHED. IMAGE DID NOT UPLOAD TO AWS S3 ==== ')
                    kwargs['logger'].warning(f"MLSNUM: {kwargs['metadata']['mlsnum']} ===== URL: {base_url} ===== ")
                    break
                else:

                    break

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

    def test_proxies(self, logger=None):

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

            except (requests.exceptions.Timeout, ProxyError, HTTPError):
                if logger is not None:
                    logger.warning(f" ==== PROXY http://{proxy_auth}@{proxy} TIMED OUT DURING TEST")
                else:
                    print(f" ==== PROXY http://{proxy_auth}@{proxy} TIMED OUT DURING TEST")
                if key not in self.dead_ips:
                    self.dead_ips.append(key)
        if logger is not None:
            logger.info(f" ==== DEAD IPS HAVE BEEN CAPTURED. RESUMING IMAGE DOWNLOADS ==== ")
        else:
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

        for category, value in image_obj.items():
            if len(value) == 0:
                update_op["$unset"].update({f"Images.{category}": ""})

    @staticmethod
    def update_mlsnum(id_num, update_op):

        if isinstance(id_num, str):
            update_op["$set"].update({"MLSNum": int(id_num)})

    @staticmethod
    def update_str_values(town_val, address_val, condition_val, zip_val, update_op):

        if town_val == town_val.upper():
            update_op["$set"].update({"Town": town_val.title()})

        if address_val == address_val.upper():
            update_op["$set"].update({"Address": address_val.title()})

        if condition_val == condition_val.upper():
            update_op["$set"].update({"Condition": condition_val.title()})

        if isinstance(zip_val, float):
            pass

        elif isinstance(zip_val, int):
            pass

        elif len(zip_val) == 4:
            update_op["$set"].update({"Zipcode": "0" + zip_val})

    """
    ----------------------------------------------------------------------------------------------------------------
                                                MAJOR FUNCTIONS
    ----------------------------------------------------------------------------------------------------------------
    """

    @logger_decorator
    def database_cleanup(self, cutoff_time, **kwargs):
        """
        Cleanup the database with the following actions:
            - Deleting duplicate documents
            - Update the date field to ISODate or Datetime formats
            - Delete the "Image_Downloaded" field if it exists. Will be replaced with "Images_Downloaded
            in a different process
            - Use title case for the Address, Town and Condition fields
            - Make the _id field the MLSNum

        :return:
        """

        logger = kwargs["logger"]
        logger.info(f" ==== CURRENT DOCUMENT COUNT FOR {self.db_name}.{self.col_name} ==== \n"
                    f" ==== TOTAL: {self.collection.count_documents({})}")

        try:
            # The cursor will die if idle for 10+ minutes. Each document result takes about 6 seconds to go through
            # the update. 600secs (10 mins) / 6 sec/doc should put us at a batch size of 100. I'll put the batchSize
            # at 85 to account for time variances
            print(' ==== GATHERING DOCUMENTS FROM AGGREGATE PIPELINE ==== ')
            for mlsnum, docs, count in self.generate_duplicate_mlsnums(cutoff_time, batch_size=10000):

                assert pendulum.now(tz=timezone("America/New_York")) < cutoff_time, \
                    f" ==== DATABASE CLEANING CUTOFF TIME HAS BEEN REACHED ==== "
                update_operation = {
                    "$set": {},  # Dictionary to hold all update operations
                    "$unset": {
                        "Image_Downloaded": ""
                    },  # Delete the fields if they exists
                }

                res = docs[0]
                address = res["Address"]
                condition = res["Condition"]
                current_doc_count = count
                date = res["Date"]
                images = res["Images"]
                town = res["Town"]
                query_filter = {"MLSNum": mlsnum}
                zipcode = res["Zipcode"]
                geo_data = res.get("Geo_data", None)

                # Log the current document information
                logger.info(f"Current document: {mlsnum}")

                # Delete duplicate documents
                self.delete_duplicates(current_doc_count, mlsnum, logger)

                # Update the longitude and latitude data
                self.update_geodata(mlsnum, geo_data, update_operation)

                # Check _id datatype, if it's a str object, switch to the int
                RealEstateImages.update_mlsnum(mlsnum, update_operation)

                # Check Date datatype
                RealEstateImages.update_date_datatype(date, update_operation)

                # Update the string values
                RealEstateImages.update_str_values(
                    town, address, condition, zipcode, update_operation
                )

                # Update the Image value to remove empty arrays
                RealEstateImages.update_image_object(images, update_operation)
                temp_dict = update_operation.copy()
                temp_dict["$unset"].pop("Image_Downloaded")

                set_operations = len(list(temp_dict['$set'].keys()))
                unset_operations = len(list(temp_dict['$unset'].keys()))

                # Update the document
                if set_operations != 0 and unset_operations != 0:
                    self.collection.update_one(query_filter, update_operation)
                    # pprint(update_operation)
                    # pprint(res)

        except CursorNotFound as cnf:
            logger.warning(f"{cnf}")
            logger.info("Starting new aggregate cursor")
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
        try:
            try:
                assert self.static_ip_status == "active", (" ==== STATIC IPS HAVE EXPIRED. "
                                                       "PURCHASE MORE DATA TO DOWNLOAD IMAGES ==== ")
            except AssertionError as e:
                print(f'{e}')
                return "Expired"

            outer_update_operation = {"$set": {"Images_Downloaded": "Yes"}}
            session = FuturesSession(max_workers=5)
            kwargs['s3_client'] = boto3.client('s3')

            for _, record in zip(tqdm(range(60), desc='Records', file=sys.stderr,
                                      dynamic_ncols=True), self.generate_image_docs()):

                assert pendulum.now(tz=timezone("America/New_York")) < cutoff_time, \
                    f" ==== IMAGE DOWNLOAD CUTOFF TIME HAS BEEN REACHED ==== "
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
            # target_date, condition = self.sql_query(target_row)
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
            self.collection.insert_one(dict(property_data))
            print(f" ==== NEW PROPERTY DOCUMENT CREATED IN MONGODB: "
                  f"{property_data['MLSNum']} - {property_data['Address']}, {property_data['Town']} ==== ")
            # print(pformat(dict(property_data)))


# if __name__ == '__main__':
#
#     obj = RealEstateImages(latest_order_num=64872924)
#     # obj.generate_current_isps()
