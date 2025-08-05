import pymongo
import urllib3.exceptions
from tqdm import tqdm
import pandas as pd
import requests
import random
import time
from datetime import datetime
from datetime import timedelta
import logging
import asyncio
import aiohttp
import numpy as np
from sqlalchemy import create_engine
from collections import defaultdict
import os
import re


# Custom class created to handle the console logging while using the tqdm progress bar
# Subclass of the logging.Handler class
class TqdmLoggingHandler(logging.Handler):

    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)

class RealEstateImages:

    def __init__(self, df_var=None):
        self.sql_conn = self.create_postgresql_conn()
        self.mongo_db_conn = self.create_mongodb_conn()
        self.proxy_check_time = datetime.now()
        self.image_df = df_var
        self.image_dir = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\MLS Photos'
        self.home_sections = {
            'Bathroom': re.compile('bath(\s)?room|bath|powder|master bath', flags=re.IGNORECASE),
            'Bedroom': re.compile('bed(\s)?room|bed|master suite|master br|master bedrm', flags=re.IGNORECASE),
            'Kitchen': re.compile('kitchen|breakfast', flags=re.IGNORECASE),
            'Garage': re.compile('garage', flags=re.IGNORECASE),
            'Front': re.compile('front yard|front(\sexterior)?', flags=re.IGNORECASE),
            'Entrance': re.compile('entrance', flags=re.IGNORECASE),
            'Foyer': re.compile('foyer', flags=re.IGNORECASE),
            'Laundry': re.compile('laundry(\sroom)?|washer|dryer', flags=re.IGNORECASE),
            'Backyard': re.compile('back(\s)?yard|rear(\sexterior)?|yard', flags=re.IGNORECASE),
            'Living Room': re.compile('living(\sroom)?|family(\sroom)?|liv rm|family rm', flags=re.IGNORECASE),
            'Basement': re.compile('basement|recreation|rec|lower level|bsmt', flags=re.IGNORECASE),
            'Gym': re.compile('exercise(\sroom)?|gym(\sroom)?', flags=re.IGNORECASE),
            'Attic': re.compile('attic', flags=re.IGNORECASE),
            'Office': re.compile('office|den', flags=re.IGNORECASE),
            'Deck': re.compile('deck|patio', flags=re.IGNORECASE),
            'Pool': re.compile('pool', flags=re.IGNORECASE),
            'Driveway': re.compile('driveway|parking', flags=re.IGNORECASE),
            'Dining Room': re.compile('dining(\sroom)?', flags=re.IGNORECASE),
            'Porch': re.compile('porch', flags=re.IGNORECASE),
            'Floor Plans': re.compile('floor plan(s)?', flags=re.IGNORECASE),
            'Tax Map': re.compile('(tax\s)?map', flags=re.IGNORECASE),
            'Sun Room': re.compile('sun(\s)?room|solarium', flags=re.IGNORECASE),
            'Alternates': re.compile('Image of listing|Image of listing.*', flags=re.IGNORECASE)
        }

    """ 
        ______________________________________________________________________________________________________________
                                       Use this section to house the decorator functions
        ______________________________________________________________________________________________________________
        """

    @staticmethod
    def logger_decorator(original_function):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(original_function.__name__)
            logger.setLevel(logging.DEBUG)
            logger.propagate = False

            if not logger.handlers:
                # Create the FileHandler() and StreamHandler() loggers
                filepath = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\MLS Photos\\Logs'
                log_filepath = os.path.join(filepath,
                                            original_function.__name__ + ' ' + str(datetime.today().date()) + '.log')
                f_handler = logging.FileHandler(log_filepath)
                f_handler.setLevel(logging.DEBUG)
                c_handler = TqdmLoggingHandler()
                c_handler.setLevel(logging.WARNING)
                # Create formatting for the loggers
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                              datefmt='%d-%b-%y %H:%M:%S')
                # Set the formatter for each handler
                f_handler.setFormatter(formatter)
                c_handler.setFormatter(formatter)
                logger.addHandler(f_handler)
                logger.addHandler(c_handler)

                kwargs['logger'] = logger
                kwargs['f_handler'] = f_handler
                kwargs['c_handler'] = c_handler

            result = original_function(*args, **kwargs)

            if result is None:
                pass
            else:
                return result

        return wrapper

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    def alternates_image_capture(self, style_type, title, property_condition, address, image_num, url, imagedict):

        if (style_type is not None) and ('Image of listing' == title) and (image_num == 0):

            self.capture_front_image_url(style_type, property_condition, address, image_num, url, imagedict)

        elif ('Image of listing' in title) and (image_num >= 0):

            for section, pattern in self.home_sections.items():
                try:
                    if pattern.search(title[16:]) is not None:

                        if section != 'Alternates':

                            if section == 'Front':

                                self.capture_front_image_url(style_type, property_condition, address,
                                                             image_num, url, imagedict)
                            else:
                                self.capture_image_url(section, property_condition, address,
                                                       image_num, url, imagedict)

                    elif (pattern.search(title[16:]) is None) and section != 'Alternates':
                        continue

                    else:
                        # The category of the image is unknown. Save and categorize it later
                        self.default_image_capture(property_condition, address,
                                                   image_num, url, imagedict)

                except IndexError:
                    self.default_image_capture(property_condition, address,
                                               image_num, url, imagedict)


    def capture_image_url(self, section_type, property_condition, address, image_num, url, imagedict):

        filename = os.path.join(self.image_dir, section_type, property_condition,
                                address + ' - ' + section_type + f'_{image_num}.png')
        imagedict[section_type].append(
            {'Condition': property_condition, 'URL': url, 'Directory': filename})

    def capture_front_image_url(self, style_type, property_condition, address, image_num, url, imagedict):

        try:
            filename = os.path.join(self.image_dir, style_type, property_condition,
                                    address + ' - ' + 'Front' + f'_{image_num}.png')
        except TypeError:
            filename = os.path.join(self.image_dir, 'Front', property_condition,
                                    address + ' - ' + 'Front' + f'_{image_num}.png')

        imagedict['Front'].append(
            {'Condition': property_condition, 'URL': url, 'Directory': filename})

    @staticmethod
    def check_for_database(db_name, connection):
        if db_name in connection.list_database_names():
            print(f"The {db_name} database exists.")
            return connection[db_name]
        else:
            # Create a database
            database = connection[db_name]
            print(f"The {db_name} database has been created.")
            return database

    @staticmethod
    def check_for_directory(directory):

        if os.path.exists(directory):
            pass
        else:
            os.makedirs(directory)
            print(f'New directory created: {directory}')

    @staticmethod
    def check_for_collection(col_name, database):
        if col_name in database.list_collection_names():
            print(f"The {col_name} collection exists.")
            return database[col_name]
        else:
            # Create a collection
            table = database['propertyImages']
            print(f"The {col_name} colletion has been created.")
            return table

    @staticmethod
    def create_new_filename(filepath, mlsnum):

        filepath_list = filepath.split('\\')
        file_address = filepath_list[-1]
        filepath_list[-1] = str(mlsnum) + ' - ' + file_address

        return '\\'.join(filepath_list)


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
    def create_postgresql_conn():
        # Create database connection
        # Adjust the database cursor to not load the whole database before chunking
        username, base_url, pw = RealEstateImages.get_us_pw('PostgreSQL-web')
        return (create_engine(f"postgresql+psycopg2://{username}:{pw}@{base_url}:5432/gsmls")
                .connect().execution_options(stream_results=True))

    @staticmethod
    def create_mongodb_conn():
        # Create database connection
        # Adjust the database cursor to not load the whole database before chunking
        # username, base_url, pw = RealEstateImages.get_us_pw('MongoDB')

        # return f'mongodb://{username}:{quote_plus(pw)}@localhost:27017/'

        return 'mongodb://localhost:27017/'

    @staticmethod
    def date_and_condition(series):

        try:

            prop_class = series['PROP_CLASS']

            if prop_class == 'RNT':
                date = series['RENTEDDATE']

                if isinstance(date, float):
                    date = '0000-00-00'
            else:
                date = series['LISTDATE']

                if isinstance(date, float):
                    date = '0000-00-00'

            condition = series['CONDITION']

            return date, condition

        except KeyError:

            return '0000-00-00', 'Unknown'

    def default_image_capture(self, property_condition, address, image_num, url, imagedict):

        filename = os.path.join(self.image_dir, 'Other', property_condition,
                                address + ' - ' + 'Other' + f'_{image_num}.png')

        imagedict['Other'].append({'Condition': property_condition, 'URL': url, 'Directory': filename})

    def generate_proxy(self, logger):

        num = random.randint(1,100)

        # Only use static proxies which have authentication to access https://img.gsmls.com
        proxy_dict = {
            1: {'proxy': '45.131.15.176:12323',
                'proxy_auth': 'user34:pwpwpw'},
            2: {'proxy': 'geo.iproyal.com:12321',
                'proxy_auth': 'EC0m7tQy2GtYN9nv:QgurSG8NEOo6TYE3_country-us_session-sRVzhKss_lifetime-30m'}
        }

        if num >= 50:
            proxy = proxy_dict[1]['proxy']
            proxy_auth = proxy_dict[1]['proxy_auth']

        else:
            proxy = proxy_dict[2]['proxy']
            proxy_auth = proxy_dict[2]['proxy_auth']

        proxies = {
            'http': f'http://{proxy_auth}@{proxy}',
            'https': f'http://{proxy_auth}@{proxy}'
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
        os.chdir('F:\\Jibreel Hameed\\Kryptonite')

        db = pd.read_excel('get_us_pw.xlsx', index_col=0)
        username = db.loc[website, 'Username']
        pw = db.loc[website, 'Password']
        base_url = db.loc[website, 'Base URL']

        os.chdir(previous_wd)

        return username, base_url, pw

    @staticmethod
    def log_proxies(proxy_dict, logger):

        proxy_list = []

        for key, value in proxy_dict.items():

            proxy = proxy_dict[key]['proxy']
            proxy_auth = proxy_dict[key]['proxy_auth']

            proxies = {
                'http': f'http://{proxy_auth}@{proxy}',
                'https': f'http://{proxy_auth}@{proxy}'
            }

            try:
                response = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=5)
                proxy_list.append(response.json()["origin"])

            except requests.exceptions.JSONDecodeError as error:
                logger.warning(f'{error} occured while checking IP for http://{proxy_auth}@{proxy}')
                logger.info(f'IP at http://{proxy_auth}@{proxy} as not added to the list')

        assert '72.90.153.78' not in proxy_list

        logger.info(f'IPs Currently in Use: {proxy_list}')
        logger.info(f'Home IP is not being traced')

    @staticmethod
    def property_style(series, prop_data):

        try:
            if series['STYLEPRIMARY_SHORT']:
                if isinstance(series['STYLEPRIMARY_SHORT'], float):
                    res_style = np.nan

                elif series['STYLEPRIMARY_SHORT'] == 'SeeRem':
                    res_style = np.nan

                else:
                    res_style = series['STYLEPRIMARY_SHORT']

        except KeyError:
            res_style = np.nan

        try:
            if series['UNITSTYLE_SHORT']:
                if isinstance(series['UNITSTYLE_SHORT'], float):
                    mul_style = np.nan

                elif series['UNITSTYLE_SHORT'] == 'SeeRem':
                    mul_style = np.nan

                else:
                    mul_style = series['UNITSTYLE_SHORT']

        except KeyError:
            mul_style = np.nan

        try:
            if series['PROPSUBTYPERN']:
                if isinstance(series['PROPSUBTYPERN'], float):
                    rnt_style = np.nan

                else:
                    rnt_style = series['PROPSUBTYPERN']

        except KeyError:
            rnt_style = np.nan

        if isinstance(res_style, float) and isinstance(mul_style, float) and isinstance(rnt_style, float):
            return None, None
        elif not isinstance(res_style, float) :
            return 'RES', RealEstateImages.style_type_split(res_style, prop_data)
        elif not isinstance(mul_style, float):
            return 'MUL', RealEstateImages.style_type_split(mul_style, prop_data)
        elif not isinstance(rnt_style, float):
            return 'RNT', RealEstateImages.style_type_split(rnt_style, prop_data)

    def request_image(self, url, filepath, logger, max_retries=None):

        try:

            response = requests.get(url, proxies=self.generate_proxy(logger), timeout=30, stream=True)

            if response.status_code == 200:
                with open(filepath, 'wb') as writer:
                    for chunk in response.iter_content(chunk_size=51200): # Stream in 50KB chunks
                        writer.write(chunk)

                logger.info(f'Image from {url} saved to {filepath}')

            else:
                response.raise_for_status()

        except requests.exceptions.ReadTimeout as error:
            logger.warning(f'Image Timeout for {url}. Error: {error}')

        except requests.exceptions.HTTPError as error:
            logger.warning(f'Request Status Code: {error} for {url}')

        # requests.exceptions.ChunkedEncodingError occurs from the urllib3.IncompleteRead when the connection closes when all chunks weren't read
        # requests.exceptions.ConnectionError occurs from SSLerror, RemoteDisconnectedError or ProxyError issues dealing with connecting to the server
        #  with the proxy
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as error:
            logger.warning(f'{error}. Max Retries: {max_retries}')

            try:
                if max_retries is not None:
                    assert max_retries <= 3

                if max_retries is None:
                    self.request_image(url, filepath,logger, 1)
                else:
                    self.request_image(url, filepath, logger, max_retries + 1)
            except AssertionError as error:
                logger.warning(f'{error} on {url}. Max Retries: {max_retries}')




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
            'RES': 'res_properties',
            'MUL': 'mul_properties',
            'RNT': 'rnt_properties'
        }

        try:

            prop_class = series['PROP_CLASS']
            mls_num = series['MLSNUM']

            if prop_class == 'RNT':
                date = series['RENTEDDATE']
            else:
                date = series['LISTDATE']


            query = f"SELECT * FROM {prop_type[prop_class]} WHERE \"MLSNUM\" = '{mls_num}';"
            data = pd.read_sql_query(query, con=self.sql_conn)
            condition = data['CONDITION'].values[0]

            return date, condition

        except KeyError:

            return '0000-00-00', 'Unknown'

    @staticmethod
    def style_type_split(style_type, prop_data):

        if (style_type is not None) and (',' in style_type):
            style_type_list = style_type.split(',')
            if 'Duplex' in style_type_list:

                return 'Duplex'

            elif 'Triplex' in style_type_list:

                return 'Triplex'

            elif 'FourPlex' in style_type_list:

                return 'FourPlex'

            elif (style_type_list[0] or style_type_list[1]) in ['Cluster', 'UndrOver', 'TwoStory', 'ThreStry', 'OneStory']:
                if 'FixrUppr' in style_type_list:
                    prop_data['Condition'] = 'FIXER UPPER'

                return 'MultiFam'

        elif style_type in ['Cluster', 'UndrOver', 'TwoStory', 'ThreStry', 'OneStory']:

            return 'MultiFam'

        elif style_type == 'Resident':

            return 'Residential'

        elif style_type == 'SeeRem':

            return None

        elif style_type == 'FixrUppr':

            prop_data['Condition'] = 'FIXER UPPER'
            return None

        else:

            return style_type

    @logger_decorator
    def download_images_main(self, **kwargs):

        logger = kwargs['logger']

        # Create a MongoDB connection
        connection = pymongo.MongoClient(self.mongo_db_conn)  # Insert connection url (ie:"mongodb://localhost:27017/")

        # Check if the database exists. If it doesn't, create it
        db_name = 'realEstate'
        database = RealEstateImages.check_for_database(db_name, connection)

        # Check if a collection (table) exists. If it doesn't, create it
        col_name = 'propertyImages'
        table = database[col_name]

        outer_update_operation = {'$set': {'Images_Downloaded': 'Yes'}}

        # Find all the records, filtering for distinct MLSNum, which haven't been downloaded yet
        batchsize = 200

        pipeline = [
            {"$match": {"Images_Downloaded": {"$exists": False}}}, # Find all records which haven't been downloaded yet
            {"$group": {
                "_id": "$MLSNum",
                "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"MLSNum": -1}},  # Sort the records in descending order by MLSNum
        ]

        results = table.aggregate(pipeline, batchSize=batchsize)

        for _, record in zip(tqdm(range(batchsize), desc='Records'), results):

            # Access the Images key in the main dictionary
            image_dict = record['Images']
            query_filter = {'MLSNum': record['MLSNum']}

            # Loop through all the image categories and access each image
            image_list = RealEstateImages.create_image_list(image_dict)

            for idx, item in zip(tqdm(range(len(image_list)), desc='Images', colour='blue'), image_list):
                url = item['URL']
                file_directory = RealEstateImages.create_new_filename(item['Directory'], record['MLSNum'])
                base_dir_list = item['Directory'].split('\\')
                base_dir = "\\".join(base_dir_list[0:-1])
                RealEstateImages.check_for_directory(base_dir)

                # Request and save the image
                if not os.path.exists(file_directory):
                    self.request_image(url, file_directory, logger)

                    # Function which introduces variability between the image requests
                    RealEstateImages.sleep_variation(idx)

            # If the key doesn't exist in the dictionary, create the field
            if record.get('Images_Downloaded') is None:
                table.update_one(query_filter, outer_update_operation)

    def main(self):
        # Create a MongoDB connection
        connection = pymongo.MongoClient(self.mongo_db_conn)  # Insert connection url (ie:"mongodb://localhost:27017/")

        # Check if the database exists. If it doesn't, create it
        db_name = 'realEstate'
        database = RealEstateImages.check_for_database(db_name, connection)

        # Check if a collection (table) exists. If it doesn't, create it
        col_name = 'propertyImages'
        table = RealEstateImages.check_for_collection(col_name, database)

        # Include a column for Property Style. Some messages won't have this data available so set to "Unknown"
        target_columns = ['MLSNum', 'ListDate', 'Address', 'Town', 'State', 'Zipcode',
                          'CountyCode', 'BlockID', 'LotID', 'Condition', 'Prop_Style', 'Images']
        image_pattern = re.compile(r"'(\d{1,5}(?:-\d{1,5}|-\w)?(?: )?(?:\w\.)? [\w+ ]*(?:\.)?, [\w+ ]*(?:\.)? - [\w+ ,&.\/!-]* - \d{0,3})': '(https:\/\/img\.gsmls\.com\/imagedb\/highres\/a\/\d{1,3}\/\d{1,15}(?:_\d{1,3})?\.jpg)'")
        # image_pattern = re.compile(r"'(\d{1,5}(?:-\d{1,5})?(?: )?(?:\w\.)? [\w+ ]*(?:\.)?, [\w+ ]*(?:\.)? - [\w+ ]* - \d{0,3})': '(https:\/\/img\.gsmls\.com\/imagedb\/highres\/a\/\d{1,3}\/\d{1,15}(?:_\d{1,3})?\.jpg)'")
        replace_pattern = re.compile("\.?\(\d{4}\)\*?")

        for _ , row_data in zip(tqdm(range(len(self.image_df)), 'Row'), self.image_df.iterrows()):

            property_data = defaultdict(str)
            target_row = row_data[1]
            target_date, condition = RealEstateImages.date_and_condition(target_row)
            # target_date, condition = self.sql_query(target_row)
            try:
                if (target_row['IMAGES'] == 'None') or isinstance(target_row['IMAGES'], float) or (image_pattern.findall(target_row['IMAGES']) == []):
                    continue
            except TypeError:
                continue

            for col in target_columns:
                if col == 'Address':
                    address = ' '.join([str(target_row['STREETNUMDISPLAY']), str(target_row['STREETNAME']).upper()])
                    property_data[col] = address
                elif col == 'MLSNum':
                    property_data[col] = target_row['MLSNUM']
                elif col == 'State':
                    property_data[col] = 'NJ'
                elif col == 'ListDate':
                    property_data['Date'] = target_date
                elif col == 'Condition':
                    property_data[col] = condition.upper()
                elif col == 'Town':
                    new_town = re.sub(replace_pattern, '', str(target_row['TOWN'])).upper()
                    property_data[col] = new_town
                elif col == 'Prop_Style':
                    type_, style_type = RealEstateImages.property_style(target_row, property_data)
                    property_data[col] = style_type

                elif col == 'Images':
                    imagedict = {}

                    for section in self.home_sections.keys():
                        imagedict.setdefault(section, [])

                    imagedict.setdefault('Other', [])

                    if isinstance(target_row['IMAGES'], str):
                        image_list = image_pattern.findall(target_row['IMAGES'])
                        for image_num, image in enumerate(image_list):

                            section = image[0].strip("'").split('-')[1].strip()
                            image_url = image[1].strip().strip("'")

                            for section_type, pattern in self.home_sections.items():
                                if pattern.search(section) is not None:
                                    if section_type != 'Alternates':

                                        if section_type == 'Front':

                                            self.capture_front_image_url(style_type, property_data['Condition'], address,
                                                               image_num, image_url, imagedict)
                                            break
                                        else:
                                            self.capture_image_url(section_type, property_data['Condition'], address,
                                                               image_num, image_url, imagedict)
                                            break
                                    else:
                                        # Image of listing is the main image title and/or there's detail about the image
                                        # in the subtext. Need to use a different method to capture the image name
                                        self.alternates_image_capture(style_type, section, property_data['Condition'],
                                                                      address, image_num, image_url, imagedict)

                                elif (pattern.search(section) is None) and section_type != 'Alternates':
                                    continue

                                else:
                                    # The category of the image is unknown. Save and categorize it later
                                    self.default_image_capture(property_data['Condition'], address,
                                                               image_num, image_url, imagedict)

                        property_data['Images'] = imagedict

                else:
                    property_data[col] = target_row[col.upper()]

            table.insert_one(dict(property_data))
            # del property_data

if __name__ == "__main__":

    current_wd = os.getcwd()
    os.chdir('F:\\Real Estate Investing\\Kafka_Data_Backups')
    # image_data = pd.read_excel('prop_images.xlsx')
    # obj = RealEstateImages(image_data)
    # obj.main()
    obj = RealEstateImages()
    obj.download_images_main()
    os.chdir(current_wd)