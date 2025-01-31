import pymongo
from tqdm import tqdm
import pandas as pd
import requests
import numpy as np
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
            'Bathroom': re.compile('bathroom|bath|powder', flags=re.IGNORECASE),
            'Bedroom': re.compile('bedroom|bed|bed room', flags=re.IGNORECASE),
            'Kitchen': re.compile('kitchen', flags=re.IGNORECASE),
            'Garage': re.compile('garage', flags=re.IGNORECASE),
            'Front': re.compile('front yard|front', flags=re.IGNORECASE),
            'Backyard': re.compile('back(\s)?yard|rear|yard', flags=re.IGNORECASE),
            'Living Room': re.compile('living(\sroom)?|family(\sroom)?', flags=re.IGNORECASE),
            'Basement': re.compile('basement|recreation|rec', flags=re.IGNORECASE),
            'Attic': re.compile('attic', flags=re.IGNORECASE),
            'Office': re.compile('office', flags=re.IGNORECASE),
            'Deck': re.compile('deck|patio', flags=re.IGNORECASE),
            'Driveway': re.compile('driveway|parking', flags=re.IGNORECASE),
            'Dining Room': re.compile('dining(\sroom)?', flags=re.IGNORECASE),
            'Porch': re.compile('porch', flags=re.IGNORECASE),
            'Alternates': re.compile('Image of listing|Image of listing.*', flags=re.IGNORECASE)
        }

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
    def date_and_condition(series):

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

        if isinstance(res_style, float) and isinstance(mul_style, float):
            return None, None
        elif not isinstance(res_style, float) :
            return 'RES', RealEstateImages.style_type_split(res_style, prop_data)
        elif not isinstance(mul_style, float):
            return 'MUL', RealEstateImages.style_type_split(mul_style, prop_data)

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

                return 'Multi-fam'

        elif style_type in ['Cluster', 'UndrOver', 'TwoStory', 'ThreStry', 'OneStory']:

            return 'Multi-fam'

        elif style_type == 'SeeRem':

            return None

        elif style_type == 'FixrUppr':

            prop_data['Condition'] = 'FIXER UPPER'
            return None

        else:

            return style_type


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
            if target_row['IMAGES'] == 'None' or isinstance(target_row['IMAGES'], float):
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
    image_data = pd.read_excel('prop_images.xlsx')
    obj = RealEstateImages(image_data)
    obj.main()
    os.chdir(current_wd)