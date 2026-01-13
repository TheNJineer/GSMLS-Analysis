from pprint import pformat
import os
import re
import requests
import random
import time
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from tqdm import tqdm
from collections import defaultdict
from pymongo.errors import CursorNotFound
from gsmls.utility_func import logger_decorator, create_sql_engine, create_mongodb_conn


class RealEstateImages:

    def __init__(self, db_name="realEstate", col_name="propertyImages", remote=True, df_var=None):
        self.db_name = db_name
        self.col_name = col_name
        self.sql_conn = create_sql_engine("nj_tax_assessor", remote=remote)
        self.mongo_db_conn = create_mongodb_conn(remote=remote)
        self.database = self.check_for_database()
        self.collection = self.check_for_collection()
        self.proxy_check_time = datetime.now()
        self.image_df = df_var
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

        filepath_list = filepath.split("\\")
        file_address = filepath_list[-1]
        filepath_list[-1] = str(mlsnum) + " - " + file_address

        return "\\".join(filepath_list)

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

    @logger_decorator
    def database_cleanup(self, **kwargs):
        """
        Cleanup the database with the following actions:
            - Deleting duplicate documents
            - Update the date field to ISODate or Datetime formats
            - Delete the "Image_Downloaded" field if it exists
            - Use title case for the Address, Town and Condition fields
            - Make the _id field the MLSNum

        :return:
        """

        logger = kwargs["logger"]
        # pipeline = [
        #     {'$match': {"Date": {"$type": "string"}}},
        #     {"$group": {"_id": {"mlsnum": "$MLSNum", "Date": "$Date", "Address": "$Address",
        #             "Town": "$Town", "Zipcode": "$Zipcode",
        #             "Condition": "$Condition", "Images": "$Images", "Geo_Data": "$Geo_Data"},  # Group common fields together
        #             "document_count": {"$count": {}},  # Count the number of duplicate documents
        #             "property_attr": {'$push': {"old_id": "$_id"}}}},  # List all the document ids of the duplicates
        #     {"$sort": {"document_count": -1}}  # Sort documents in descending order
        # ]

        pipeline = [
            {"$match": {"Date": {"$type": "string"}}},
            {
                "$group": {"_id": "$MLSNum"},
                "Date": {"$push": "$Date"},
                "Address": {"$push": "$Address"},
                "Town": {"$push": "$Town"},
                "Zipcode": {"$push": "$Zipcode"},
                "Condition": {"$push": "$Condition"},
                "Images": {"$push": "$Images"},
                "Geo_Data": {
                    "$push": {"$ifNull": ["$Geo_Data", "$$REMOVE"]}
                },  # If field doesn't exist then remove from results
                "document_count": {
                    "$count": {}
                },  # Count the number of duplicate documents
                "property_attr": {"$push": {"old_id": "$_id"}},
            },  # List all the document ids of the duplicates
            {"$sort": {"document_count": -1}},  # Sort documents in descending order
        ]

        logger.info(
            f"Current document count for {self.db_name} collection: {self.collection.count_documents({})}"
        )

        while True:

            try:
                # The cursor will die if idle for 10+ minutes. Each document result takes about 6 seconds to go through
                # the update. 600secs (10 mins) / 6 sec/doc should put us at a batch size of 100. I'll put the batchSize
                # at 85 to account for time variances
                for res in tqdm(
                    self.collection.aggregate(
                        pipeline, batchSize=85, allowDiskUse=True
                    ),
                    desc="Records",
                ):

                    update_operation = {
                        "$set": {},  # Dictionary to hold all update operations
                        "$unset": {
                            "Image_Downloaded": ""
                        },  # Delete the fields if they exists
                    }

                    address = res["_id"]["Address"]
                    condition = res["_id"]["Condition"]
                    current_doc_count = res["document_count"]
                    date = res["_id"]["Date"]
                    images = res["_id"]["Images"]
                    old_id = res["property_attr"][-1]["old_id"]
                    targ_id = res["_id"]["mlsnum"]
                    town = res["_id"]["Town"]
                    query_filter = {"MLSNum": targ_id}
                    zipcode = res["_id"]["Zipcode"]
                    geo_data = res["_id"].get("Geo_data", None)

                    # Log the current document information
                    logger.info(f"Current document: {targ_id}")

                    # Delete duplicate documents
                    self.delete_duplicates(current_doc_count, targ_id, old_id, logger)

                    # Update the longitude and latitude data
                    self.update_geodata(targ_id, geo_data, update_operation)

                    # Check _id datatype, if it's a str object, switch to the int
                    RealEstateImages.update_mlsnum(targ_id, update_operation)

                    # Check Date datatype
                    RealEstateImages.update_date_datatype(date, update_operation)

                    # Update the string values
                    RealEstateImages.update_str_values(
                        town, address, condition, zipcode, update_operation
                    )

                    # Update the Image value to remove empty arrays
                    RealEstateImages.update_image_object(images, update_operation)

                    # Update the document
                    self.collection.update_one(query_filter, update_operation)

            except CursorNotFound as cnf:
                logger.warning(f"{cnf}")
                logger.info("Starting new aggregate cursor")
                continue
            else:
                logger.info("Database cleanup has been completed")
                break

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

    def delete_duplicates(self, doc_count, id_num, old_id, logger):

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
            logger.info(f"The last existing document ObjectID for {id_num} is {old_id}")

        else:
            logger.info(f"No duplicate documents for {id_num}")

    def generate_proxy(self, logger):

        num = random.randint(1, 100)

        # Only use static proxies which have authentication to access https://img.gsmls.com
        proxy_dict = {
            1: {"proxy": "45.131.15.176:12323", "proxy_auth": "user34:pwpwpw"},
            2: {
                "proxy": "geo.iproyal.com:12321",
                "proxy_auth": "EC0m7tQy2GtYN9nv:QgurSG8NEOo6TYE3_country-us_session-sRVzhKss_lifetime-30m",
            },
        }

        if num >= 50:
            proxy = proxy_dict[1]["proxy"]
            proxy_auth = proxy_dict[1]["proxy_auth"]

        else:
            proxy = proxy_dict[2]["proxy"]
            proxy_auth = proxy_dict[2]["proxy_auth"]

        proxies = {
            "http": f"http://{proxy_auth}@{proxy}",
            "https": f"http://{proxy_auth}@{proxy}",
        }

        if datetime.now() >= self.proxy_check_time + timedelta(minutes=30):
            self.proxy_check_time = datetime.now()
            RealEstateImages.log_proxies(proxy_dict, logger)

        return proxies

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
    def log_proxies(proxy_dict, logger):

        proxy_list = []

        for key, value in proxy_dict.items():

            proxy = proxy_dict[key]["proxy"]
            proxy_auth = proxy_dict[key]["proxy_auth"]

            proxies = {
                "http": f"http://{proxy_auth}@{proxy}",
                "https": f"http://{proxy_auth}@{proxy}",
            }

            try:
                response = requests.get(
                    "https://httpbin.org/ip", proxies=proxies, timeout=5
                )
                proxy_list.append(response.json()["origin"])

            except requests.exceptions.JSONDecodeError as error:
                logger.warning(
                    f"{error} occured while checking IP for http://{proxy_auth}@{proxy}"
                )
                logger.info(
                    f"IP at http://{proxy_auth}@{proxy} as not added to the list"
                )

        assert "72.90.153.78" not in proxy_list

        logger.info(f"IPs Currently in Use: {proxy_list}")
        logger.info(f"Home IP is not being traced")

    @staticmethod
    def property_style(series, prop_data):

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

    def request_image(self, url, filepath, logger, max_retries=None):

        try:

            response = requests.get(
                url, proxies=self.generate_proxy(logger), timeout=30, stream=True
            )

            if response.status_code == 200:
                with open(filepath, "wb") as writer:
                    for chunk in response.iter_content(
                        chunk_size=51200
                    ):  # Stream in 50KB chunks
                        writer.write(chunk)

                logger.info(f"Image from {url} saved to {filepath}")

            else:
                response.raise_for_status()

        except requests.exceptions.ReadTimeout as error:
            logger.warning(f"Image Timeout for {url}. Error: {error}")

        except requests.exceptions.HTTPError as error:
            logger.warning(f"Request Status Code: {error} for {url}")

        # requests.exceptions.ChunkedEncodingError occurs from the urllib3.IncompleteRead when the connection closes when all chunks weren't read
        # requests.exceptions.ConnectionError occurs from SSLerror, RemoteDisconnectedError or ProxyError issues dealing with connecting to the server
        #  with the proxy
        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
        ) as error:
            logger.warning(f"{error}. Max Retries: {max_retries}")

            try:
                if max_retries is not None:
                    assert max_retries <= 3

                if max_retries is None:
                    self.request_image(url, filepath, logger, 1)
                else:
                    self.request_image(url, filepath, logger, max_retries + 1)
            except AssertionError as error:
                logger.warning(f"{error} on {url}. Max Retries: {max_retries}")

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
    def style_type_split(style_type, prop_data):

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

    @logger_decorator
    def download_images_main(self, **kwargs):
        """
        Queries each document and downloads the images stored in the Images field

        :param kwargs:
        :return:
        """

        logger = kwargs["logger"]

        # Check if the database exists. If it doesn't, create it
        db_name = "realEstate"
        database = RealEstateImages.check_for_database(db_name, self.mongo_db_conn)

        # Check if a collection (table) exists. If it doesn't, create it
        col_name = "propertyImages"
        table = database[col_name]

        outer_update_operation = {"$set": {"Images_Downloaded": "Yes"}}

        # Find all the records, filtering for distinct MLSNum, which haven't been downloaded yet
        batchsize = 200

        pipeline = [
            {
                "$match": {"Images_Downloaded": {"$exists": False}}
            },  # Find all records which haven't been downloaded yet
            {"$group": {"_id": "$MLSNum", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"MLSNum": -1}},  # Sort the records in descending order by MLSNum
        ]

        results = table.aggregate(pipeline, batchSize=batchsize)

        for _, record in zip(tqdm(range(batchsize), desc="Records"), results):

            # Access the Images key in the main dictionary
            image_dict = record["Images"]
            query_filter = {"MLSNum": record["MLSNum"]}

            # Loop through all the image categories and access each image
            image_list = RealEstateImages.create_image_list(image_dict)

            for idx, item in zip(
                tqdm(range(len(image_list)), desc="Images", colour="blue"), image_list
            ):
                url = item["URL"]
                file_directory = RealEstateImages.create_new_filename(
                    item["Directory"], record["MLSNum"]
                )
                base_dir_list = item["Directory"].split("\\")
                base_dir = "\\".join(base_dir_list[0:-1])
                RealEstateImages.check_for_directory(base_dir)

                # Request and save the image
                if not os.path.exists(file_directory):
                    self.request_image(url, file_directory, logger)

                    # Function which introduces variability between the image requests
                    RealEstateImages.sleep_variation(idx)

            # If the key doesn't exist in the dictionary, create the field
            if record.get("Images_Downloaded") is None:
                table.update_one(query_filter, outer_update_operation)

    def main(self, **kwargs):
        """
        Stores real estate property image data from a Pandas dataframe
        :return:
        """

        # image_pattern = re.compile(
        #     r"'(\d{1,5}(?:-\d{1,5}|-\w)?(?: )?(?:\w\.)? [\w+ ]*(?:\.)?, [\w+ ]*(?:\.)? - [\w+ ,&.\/!-]* - \d{0,3})': '(https:\/\/img\.gsmls\.com\/imagedb\/highres\/a\/\d{1,3}\/\d{1,15}(?:_\d{1,3})?\.jpg)'"
        # )
        image_pattern = re.compile(r"'([^']+?)'\s*:\s*'(https:\/\/img\.gsmls\.com\/imagedb\/highres\/[^']+?\.jpg)'")
        kwargs["image_pattern"] = image_pattern

        for _, row_data in zip(tqdm(range(len(self.image_df)), "Row"), self.image_df.iterrows()):

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
            # self.collection.insert_one(dict(property_data))
            print(f" ==== NEW PROPERTY DOCUMENT CREATED IN MONGODB: "
                  f"{property_data['MLSNum']} - {property_data['Address']}, {property_data['Town']} ==== ")
            print(pformat(dict(property_data)))
