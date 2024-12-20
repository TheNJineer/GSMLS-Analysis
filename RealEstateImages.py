import pymongo
import pandas as pd
import requests
from sqlalchemy import create_engine
from collections import defaultdict
import os


class RealEstateImages:

    def __init__(self):
        self.sql_conn = self.create_postgresql_conn()
        self.mongo_db_conn = self.create_mongodb_conn()
        self.image_dir = 'F:\\my\\image\\directory'
        try:
            self.last_run = self.last_scrape_date()
        except BaseException:
            self.last_run = None

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
        username, base_url, pw = RealEstateImages.get_us_pw('PostgreSQL')
        return (create_engine(f"postgresql+psycopg2://{username}:{pw}@{base_url}:5432/gsmls")
                .connect().execution_options(stream_results=True))

    @staticmethod
    def create_mongodb_conn():
        # Create database connection
        # Adjust the database cursor to not load the whole database before chunking
        _, base_url, _ = RealEstateImages.get_us_pw('Mongo DB')
        return base_url

    def create_chunk_size(self):
        # Create the chunksize of rows to query
        # Used to save memory when loading data
        if self.last_run is not None:
            rows = pd.read_sql_query(f"SELECT COUNT(address) FROM res_sold_properties WHERE date_added => run_date",
                                     self.sql_conn)
        else:
            rows = pd.read_sql_query(f"SELECT COUNT(address) FROM res_sold_properties", self.sql_conn)

        if rows < 10000:
            return rows // 3
        elif 10000 < rows < 35000:
            return rows // 10
        elif 35000 < rows < 99000:
            return rows // 30
        elif 99000 < rows < 150000:
            return rows // 50
        elif 150000 < rows < 350000:
            return rows // 100
        elif 350000 < rows < 550000:
            return rows // 150
        else:
            return rows // 300

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

    def last_scrape_date(self):
        # Query the date of the last time this program was run
        # If it hasnt run before, return None
        return pd.read_sql_query("SELECT run_date FROM event_log ORDER BY run_date DESC LIMIT 1", self.sql_conn)


    def main(self):
        # Create a MongoDB connection
        connection = pymongo.MongoClient(self.mongo_db_conn)  # Insert connection url (ie:"mongodb://localhost:27017/")

        # Check if the database exists. If it doesn't, create it
        db_name = 'realEstate'
        database = RealEstateImages.check_for_database(db_name, connection)

        # Check if a collection (table) exists. If it doesn't, create it
        col_name = 'propertyImages'
        table = RealEstateImages.check_for_collection(col_name, database)

        if self.last_run is None:
            query = "SELECT * FROM res_sold_properties"
        else:
            query = f"SELECT * FROM res_sold_properties WHERE date_added => run_date"

        chunk_size = self.create_chunk_size()

        target_columns = ['MLSNum', 'Address', 'Town', 'State', 'Zipcode', 'Block_ID', 'Lot_ID', 'Condition', 'Images']
        home_sections = ['Bathroom', 'Bedroom', 'Kitchen', 'Garage', 'Front', 'Backyard', 'Living Room']

        # The chunksize arg turns the query into an iterator that returns the results in batches
        # Read data from PySpark + Kafka stream instead of pd.read_sql_query()
        for chunk in pd.read_sql_query(query, self.sql_conn, chunksize=chunk_size):
            property_data = defaultdict(str)
            for _, row in chunk.iterrows():
                for col in target_columns:
                    if col == 'Address':
                        address = ' '.join([row['StreetDisplayNum'], row['StreetName']])
                        property_data['Address'] = address
                    elif col == 'MLSNum':
                        property_data['_id'] = row['MLSNum']
                    elif col == 'State':
                        property_data['State'] = 'NJ'
                    elif col == 'Images':
                        imagedict = {}

                        for section in home_sections:
                            imagedict.setdefault(section, [])
                    
                        imagedict.setdefault('Other', [])

                        for image in row['Images']:
                            section = image.split('-').strip()[0]
                            image_url = image.split('-').strip()[1]
                            filename = os.path.join(self.image_dir, row['Condition'], section, address + ' - ' + section.png)

                            response = requests.get(image_url)

                            if response.status_code == 200:
                                # stream the binary data into a png
                                if section.title() in home_sections:
                                    imagedict[section].append({'Condition': 'Unknown', 'Directory': filename})
                                else:
                                    imagedict['Other'].append({'Condition': 'Unknown', 'Directory': filename})
                            else:
                                # Say images wasn't downloaded
                                property_data['Images'] = imagedict
                    else:
                        property_data[col] = row[col]

                    table.insert_one(dict(property_data))

if __name__ == "__main__":
    obj = RealEstateImages()
    obj.main()