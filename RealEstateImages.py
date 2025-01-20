import pymongo
from tqdm import tqdm
import pandas as pd
import requests
import numpy as np
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from collections import defaultdict
import os
import re


class RealEstateImages:

    def __init__(self, df_var):
        self.sql_conn = self.create_postgresql_conn()
        self.mongo_db_conn = self.create_mongodb_conn()
        self.image_df = df_var
        self.image_dir = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\MLS Photos'
        self.home_sections = {
            'Bathroom': re.compile('bathroom|bath', flags=re.IGNORECASE),
            'Bedroom': re.compile('bedroom|bed', flags=re.IGNORECASE),
            'Kitchen': re.compile('kitchen', flags=re.IGNORECASE),
            'Garage': re.compile('garage', flags=re.IGNORECASE),
            'Front': re.compile('front yard|front', flags=re.IGNORECASE),
            'Backyard': re.compile('back(\s)?yard|rear|yard', flags=re.IGNORECASE),
            'Living Room': re.compile('living(\sroom)?|family(\sroom)?', flags=re.IGNORECASE),
            'Basement': re.compile('basement', flags=re.IGNORECASE),
            'Attic': re.compile('attic', flags=re.IGNORECASE),
            'Office': re.compile('office', flags=re.IGNORECASE)
        }

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
    def check_for_collection(col_name, database):
        if col_name in database.list_collection_names():
            print(f"The {col_name} database exists.")
            return database[col_name]
        else:
            # Create a collection
            table = database['propertyImages']
            print(f"The {col_name} colletion has been created.")
            return table

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
    def property_style(series):

        try:
            if series['STYLEPRIMARY_SHORT']:
                res_style = series['STYLEPRIMARY_SHORT']
        except KeyError:
            res_style = None

        try:
            if series['UNITSTYLE_SHORT']:
                mul_style = series['UNITSTYLE_SHORT']
        except KeyError:
            mul_style = None

        if (res_style and mul_style) is (None or np.nan):
            return None
        elif res_style is not (None or np.nan) :
            return res_style
        elif mul_style is not (None or np.nan):
            return mul_style

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


            query = f'SELECT * FROM {prop_type[prop_class]} WHERE "MLSNUM" = {mls_num}'
            data = pd.read_sql_query(query, con=self.sql_conn)

            return date, data['CONDITION']

        except KeyError:

            return '0000-00-00', 'Unknown'


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
                          'CountyCode', 'BlockID', 'LotID', 'Condition', 'Images', 'Prop_Style']

        image_pattern = re.compile(r"('.*'):(.*')")
        replace_pattern = re.compile("\.?\(\d{4}\)\*?")

        for _ , row_data in zip(tqdm(range(len(self.image_df)), 'Row'), self.image_df.iterrows()):

            property_data = defaultdict(str)
            target_row = row_data[1]
            target_date, condition = self.sql_query(target_row)

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
                    property_data[col] = RealEstateImages.property_style(target_row)
                elif col == 'Images':
                    imagedict = {}

                    for section in self.home_sections.keys():
                        imagedict.setdefault(section, [])

                    imagedict.setdefault('Other', [])

                    if isinstance(target_row['IMAGES'], str):
                        image_list = image_pattern.findall(target_row['IMAGES'])
                        for image in image_list:

                            section = image[0].strip("'").split('-')[1].strip()
                            image_url = image[1].strip().strip("'")

                            for section_type, pattern in self.home_sections.items():
                                if pattern.search(section) is not None:
                                    filename = os.path.join(self.image_dir, condition, section,
                                                            address + ' - ' + section + '.png')
                                    imagedict[section_type].append(
                                        {'Condition': condition, 'URL': image_url, 'Directory': filename})
                                    break
                                elif (pattern.search(section) is None) and section_type != 'Office':
                                    continue
                                else:
                                    filename = os.path.join(self.image_dir, condition, 'Other',
                                                            address + ' - ' + section + '.png')
                                    imagedict['Other'].append(
                                        {'Condition': condition, 'URL': image_url, 'Directory': filename})


                        property_data['Images'] = imagedict

                else:
                    property_data[col] = target_row[col.upper()]

            table.insert_one(dict(property_data))

if __name__ == "__main__":

    current_wd = os.getcwd()
    os.chdir('F:\\Real Estate Investing')
    image_data = pd.read_excel('prop_images.xlsx')
    obj = RealEstateImages(image_data)
    obj.main()
    os.chdir(current_wd)