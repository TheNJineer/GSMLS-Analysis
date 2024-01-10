import re
import time
import os
import requests
import shelve
import datetime
import traceback
import pandas as pd
import numpy as np
from NJTaxAssessment_v2 import NJTaxAssessment
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import selenium
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains as AC
# Allows for Selenium to click a button
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotSelectableException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import NoSuchAttributeException
from selenium.common.exceptions import NoSuchDriverException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException


class GSMLS:

    def __init__(self):
        # What information do I need to initialize an instance of this class?
        pass

    """ 
    ______________________________________________________________________________________________________________
                                   Use this section to house the decorator functions
    ______________________________________________________________________________________________________________
    """
    @staticmethod
    def clean_db_decorator(original_function):
        def wrapper(*args, **kwargs):

            logger = kwargs['logger']
            f_handler = kwargs['f_handler']
            c_handler = kwargs['c_handler']

            res_db_list = []
            mul_db_list = []
            lnd_db_list = []
            path = 'C:\\Users\\Omar\\Desktop\\STF'
            os.chdir(path)
            dirty_dbs_list = os.listdir(path)

            for file in dirty_dbs_list:
                if file.endswith('.xlsx'):
                    db = pd.read_excel(file, engine='openpyxl')
                    city_name = db.loc[0, 'TOWN'].rstrip('*1234567890()')
                    for ending in ['Town', 'Twp.', 'Boro', 'City']:
                        if ending in city_name:
                            city_name = city_name.split(ending)[0].strip()
                    county_name = db.loc[0, 'COUNTY'].rstrip('*')
                    property_type = file.split(' ')[-3]

                    tax_db = NJTaxAssessment.city_database(county_name, city_name)
                    tax_db.set_index('Property Location')

                    kwargs['db'] = db
                    kwargs['tax_db'] = tax_db
                    kwargs['property_type'] = property_type

                    result = original_function(*args, **kwargs)

                    if property_type == 'RES':
                        res_db_list.append(result)

                    elif property_type == 'MUL':
                        mul_db_list.append(result)

                    elif property_type == 'LND':
                        lnd_db_list.append(result)

                else:
                    continue

            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

        return wrapper




    @staticmethod
    def logger_decorator(original_function):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(original_function.__name__)
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
            # Create the FileHandler() and StreamHandler() loggers
            f_handler = logging.FileHandler(
                original_function.__name__ + ' ' + str(datetime.today().date()) + '.log')
            f_handler.setLevel(logging.DEBUG)
            c_handler = logging.StreamHandler()
            c_handler.setLevel(logging.INFO)
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

    @staticmethod
    def quarterly_sales(original_function):
        def wrapper(*args, **kwargs):

            logger = kwargs['logger']
            f_handler = kwargs['f_handler']
            c_handler = kwargs['c_handler']

            property_type = str(original_function.__name__)[-3:].upper()

            time_periods = {
                'Q1': ['01/01/' + str(datetime.today().year), '03/31/' + str(datetime.today().year)],
                'Q2': ['04/01/' + str(datetime.today().year), '06/30/' + str(datetime.today().year)],
                'Q3': ['07/01/' + str(datetime.today().year), '09/30/' + str(datetime.today().year)],
                'Q4': ['10/01/' + str(datetime.today().year), '12/31/' + str(datetime.today().year)]
            }

            # time_periods = {
            #     'Q1': ['01/01/2023', '03/31/2023'],
            #     'Q2': ['04/01/2023', '06/30/2023'],
            #     'Q3': ['07/01/2023', '09/30/2023'],
            #     'Q4': ['10/01/2023', '12/31/2023']
            # }

            run_log = GSMLS.open_run_log()

            for qtr, date_range in time_periods.items():
                kwargs['Qtr'] = qtr
                kwargs['Dates'] = date_range
                kwargs['Run Log'] = run_log
                if datetime.today() >= datetime.strptime(date_range[1], '%m/%d/%Y'):

                    if run_log[property_type][qtr] == 'D.N.A':
                        # D.N.A means 'Data Not Available'
                        run_log = original_function(*args, **kwargs)

                    elif run_log[property_type][qtr] == 'IN PROGRESS':
                        # run modified_quarterly_download
                        previous_dir = os.getcwd()
                        path = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'
                        os.chdir(path)
                        latest_file = sorted(os.listdir(path), key=lambda x: os.path.getctime(x))[-1]
                        db = pd.read_excel(latest_file)
                        os.chdir(previous_dir)
                        kwargs['city_name'] = latest_file.split('Q')[0].strip()
                        kwargs['county_name'] = db.loc[0, 'COUNTY'].rstrip('*')
                        run_log = original_function(*args, **kwargs)

                    elif run_log[property_type][qtr] == 'DOWNLOADED':
                        logger.info(f'The {property_type} data has already been downloaded for {qtr}')
                        pass

                else:
                    # May need to put a logger msg here
                    # May need to break the code here. No sense of continuing the loop if all subsequent data isnt
                    # available
                    continue

            GSMLS.check_run_log(run_log, logger)

            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

        return wrapper

    @staticmethod
    def run_main(original_function):
        def wrapper(*args, **kwargs):
            pass
            # Formulate all the date variables
            # todays_date = datetime.datetime.today().date()
            # data_avail = Scraper.current_data
            # temp_date = str(todays_date).split('-')
            # day = int(temp_date[2])
            # month = int(temp_date[1])
            # year = temp_date[0]
            # current_run_date = datetime.datetime.strptime(year + '-' + temp_date[1] + '-' + '24', "%Y-%m-%d").date()
            #
            # # Logic for calculating the next date to run main()
            # if day < 24:
            #     next_run_date = year + '-' + temp_date[1] + '-' + '24'
            # elif day >= 24:
            #     if data_avail == Scraper.event_log[obj.no_of_runs - 1]['Latest Available Data']:
            #         next_run_date = year + '-' + temp_date[1] + '-' + '24'
            #     else:
            #         if month in [1, 2, 3, 4, 5, 6, 7, 8]:
            #             nm = str(month + 1)
            #             next_month = '0' + nm
            #             next_run_date = year + '-' + next_month + '-' + '24'
            #         elif month in [9, 10, 11]:
            #             next_month = str(month + 1)
            #             next_run_date = year + '-' + next_month + '-' + '24'
            #         elif month == 12:
            #             next_month = '01'
            #             year = str(int(temp_date[0]) + 1)
            #             next_run_date = year + '-' + next_month + '-' + '24'
            #
            # next_run_date = datetime.datetime.strptime(next_run_date, "%Y-%m-%d").date()
            # if todays_date >= current_run_date:
            #     if data_avail == Scraper.event_log[Scraper.no_of_runs - 1]['Latest Available Data']:
            #         sleep_time = timedelta(days=1)
            #         Scraper.waiting(sleep_time)
            #
            #         return 'RESTART'
            #
            #     else:
            #         good_to_go = original_function(*args, **kwargs)
            #
            #     return good_to_go
            #
            # elif current_run_date < todays_date < next_run_date:
            #     if todays_date < next_run_date:
            #         sleep_time = next_run_date - todays_date
            #         Scraper.waiting(sleep_time)
            #
            #         return 'RESTART'

        return wrapper

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    @staticmethod
    def acres_to_sqft(search_string):
        return str(float(search_string.group(1)) * 43560)

    def area_demographics(self, city):
        # Create a method that generates a report on the stores in or near a city,
        # school rankings, walk score, public transportation
        pass

    def available_inventory(self, city=None):
        """
        Checks the available inventory in that city and checks the percentage of homes which have
        decreased/increased in price and the percentage of the avg increase/decrease with respect to
        the original LP
        :param city:
        :return:
        """
        pass

    @staticmethod
    def check_run_log(run_log_object: dict, logger):

        count = 0

        for prop_type, run_status in run_log_object.items():
            for status in run_status.values():
                if status == 'DOWNLOADED':
                    count += 1
                else:
                    pass

        if count == 12:
            for prop_type, run_status in run_log_object.items():
                for qtr in run_status.keys():
                    run_log_object[prop_type][qtr] = 'D.N.A'

            information = f'The sales data for all quarters have been downloaded. The run log has been reset'
            GSMLS.save_run_log(run_log_object, qtr, prop_type, "Doesn't matter", logger, message=information)

    def check_status(self):
        # Checks the action buttons to filter to the status of the homes we want to look up
        pass

    @staticmethod
    def cities_download_manager(counties, county_id, driver_var, logger):

        logger.info(f'Sales data for municipalities located in {counties[county_id]} '
                    f'County will now be downloaded')
        GSMLS.set_county(county_id, driver_var)
        time.sleep(1)  # Latency period added in order to load and scrape city names
        results1 = driver_var.page_source

        return GSMLS.find_cities(results1)

    @staticmethod
    def clean_and_transform_data_lnd(pandas_db):
        """
        Cleaning that needs to be done
        1. Filter for columns that I want displayed
        2. Remove the asterics attached to the following columns:
            BLOCKID, COUNTY, LOTSIZE, LOTID, STREETNAME
        3. Remove the *(NNNN*) from the town name
        4. Make LISTDATE, PENDINGDATE, CLOSEDDATE columns date type
        5. If no POOL, fillna with 'N' and POOLDESC with 'N'
        6. Create ADDRESS column by combining the 'STREETNUMDISPLAY' and 'STREETNAME' columns
        7. Create 'LATITUDE' AND 'LONGITUTDE' columns and fill with 'N/A'. Move columns right before ADDRESS column
        8. Add a column named "UC-Days" which calculates the total days between going under contract and closing
            # Can be vectorized by doing db['UC-Days'] = db['Closing Date'] - db['Under Contract']
        9. Convert all the values in the LOTSIZE column to sqft. Use Pandas str methods
        :param pandas_db:
        :return:
        """

        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'ORIGLISTPRICE': 'int64', 'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'SPLP': 3})

        # List item 2
        pandas_db.insert(2, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(3, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(4, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(6, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(12, 'SALESPRICE', pandas_db.pop('SALESPRICE'))
        pandas_db.insert(13, 'SPLP', pandas_db.pop('SPLP'))
        pandas_db.insert(14, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))
        pandas_db.insert(17, 'LOTDESC', pandas_db.pop('LOTDESC'))

        # List item 3
        pandas_db.insert(5, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
        # List item 4 and 8
        pandas_db.insert(26, 'LISTDATE', pd.to_datetime(pandas_db.pop('LISTDATE')))
        pandas_db.insert(27, 'PENDINGDATE', pd.to_datetime(pandas_db.pop('PENDINGDATE')))
        pandas_db.insert(28, 'CLOSEDDATE', pd.to_datetime(pandas_db.pop('CLOSEDDATE')))
        pandas_db.insert(29, 'UNDER CONTRACT LENGTH', pandas_db['CLOSEDDATE'] - pandas_db['PENDINGDATE'])

        # List item 6
        street_num = pandas_db.pop('STREETNUMDISPLAY')
        street_add = pandas_db.pop('STREETNAME')
        pandas_db.insert(3, 'ADDRESS', street_num.str.cat(street_add, join='left', sep=' ')
                         .str.replace(r'Rd$', 'Road', regex=True)
                         .str.replace(r'Ct$', 'Court', regex=True)
                         .str.replace(r'St$', 'Street', regex=True)
                         .str.replace(r'Ave$', 'Avenue', regex=True)
                         .str.replace(r'Dr$', 'Drive', regex=True)
                         .str.replace(r'Ln$', 'Lane', regex=True)
                         .str.replace(r'Pl$', 'Place', regex=True)
                         .str.replace(r'Ter$', 'Terrace', regex=True)
                         .str.replace(r'Hwy$', 'Highway', regex=True)
                         .str.replace(r'Pkwy$', 'Parkway', regex=True)
                         .str.replace(r'Cir$', 'Circle', regex=True))
        # List item 7
        pandas_db.insert(3, 'LATITUDE', 0)
        pandas_db.insert(4, 'LONGITUDE', 0)

        return pandas_db

    @staticmethod
    def clean_and_transform_data_mul(pandas_db):
        """
        Cleaning that needs to be done
        1. Filter for columns that I want displayed
        2. Remove the asterics attached to the following columns:
            BLOCKID, COUNTY, LOTSIZE, LOTID, STREETNAME
        3. Remove the *(NNNN*) from the town name
        4. Make LISTDATE, PENDINGDATE, CLOSEDDATE columns date type
        5. If no POOL, fillna with 'N' and POOLDESC with 'N'
        6. Create ADDRESS column by combining the 'STREETNUMDISPLAY' and 'STREETNAME' columns
        7. Create 'LATITUDE' AND 'LONGITUTDE' columns and fill with 'N/A'. Move columns right before ADDRESS column
        8. Add a column named "UC-Days" which calculates the total days between going under contract and closing
            # Can be vectorized by doing db['UC-Days'] = db['Closing Date'] - db['Under Contract']
        9. Convert all the values in the LOTSIZE column to sqft. Use Pandas str methods
        :param pandas_db:
        :return:
        """

        pandas_db['RENOVATED'] = pandas_db['RENOVATED'].fillna(0)
        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'RENOVATED': 'int64', 'ORIGLISTPRICE': 'int64',
                                      'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'BATHSTOTAL': 1, 'SPLP': 3})

        # List item 2
        pandas_db.insert(2, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(3, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(4, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(6, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(11, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))

        # List item 3
        pandas_db.insert(5, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
        # List item 4 and 8
        pandas_db.insert(26, 'LISTDATE', pd.to_datetime(pandas_db.pop('LISTDATE')))
        pandas_db.insert(27, 'PENDINGDATE', pd.to_datetime(pandas_db.pop('PENDINGDATE')))
        pandas_db.insert(28, 'CLOSEDDATE', pd.to_datetime(pandas_db.pop('CLOSEDDATE')))
        pandas_db.insert(29, 'UNDER CONTRACT LENGTH', pandas_db['CLOSEDDATE'] - pandas_db['PENDINGDATE'])

        # List item 6
        street_num = pandas_db.pop('STREETNUMDISPLAY')
        street_add = pandas_db.pop('STREETNAME')
        pandas_db.insert(3, 'ADDRESS', street_num.str.cat(street_add, join='left', sep=' ')
                         .str.replace(r'Rd$', 'Road', regex=True)
                         .str.replace(r'Ct$', 'Court', regex=True)
                         .str.replace(r'St$', 'Street', regex=True)
                         .str.replace(r'Ave$', 'Avenue', regex=True)
                         .str.replace(r'Dr$', 'Drive', regex=True)
                         .str.replace(r'Ln$', 'Lane', regex=True)
                         .str.replace(r'Pl$', 'Place', regex=True)
                         .str.replace(r'Ter$', 'Terrace', regex=True)
                         .str.replace(r'Hwy$', 'Highway', regex=True)
                         .str.replace(r'Pkwy$', 'Parkway', regex=True)
                         .str.replace(r'Cir$', 'Circle', regex=True))
        # List item 7
        pandas_db.insert(3, 'LATITUDE', 0)
        pandas_db.insert(4, 'LONGITUDE', 0)

        return pandas_db

    @staticmethod
    def clean_and_transform_data_res(pandas_db):
        """
        Cleaning that needs to be done
        1. Filter for columns that I want displayed
        2. Remove the asterics attached to the following columns:
            BLOCKID, COUNTY, LOTSIZE, LOTID, STREETNAME
        3. Remove the *(NNNN*) from the town name
        4. Make LISTDATE, PENDINGDATE, CLOSEDDATE columns date type
        5. If no POOL, fillna with 'N' and POOLDESC with 'N'
        6. Create ADDRESS column by combining the 'STREETNUMDISPLAY' and 'STREETNAME' columns
        7. Create 'LATITUDE' AND 'LONGITUTDE' columns and fill with 'N/A'. Move columns right before ADDRESS column
        8. Add a column named "UC-Days" which calculates the total days between going under contract and closing
            # Can be vectorized by doing db['UC-Days'] = db['Closing Date'] - db['Under Contract']
        9. Convert all the values in the LOTSIZE column to sqft. Use Pandas str methods
        :param pandas_db:
        :return:
        """

        pandas_db['SQFTAPPROX'] = pandas_db['SQFTAPPROX'].fillna(0)
        pandas_db['RENOVATED'] = pandas_db['RENOVATED'].fillna(0)
        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'SQFTAPPROX': 'int64', 'RENOVATED': 'int64', 'ORIGLISTPRICE': 'int64',
                                      'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'BATHSTOTAL': 1, 'SPLP': 3})

        # List item 2
        pandas_db.insert(2, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(3, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(4, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(6, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(11, 'SQFTAPPROX', pandas_db.pop('SQFTAPPROX'))
        pandas_db.insert(12, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))

        # List item 3
        pandas_db.insert(5, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
        # List item 4 and 8
        pandas_db.insert(26, 'LISTDATE', pd.to_datetime(pandas_db.pop('LISTDATE')))
        pandas_db.insert(27, 'PENDINGDATE', pd.to_datetime(pandas_db.pop('PENDINGDATE')))
        pandas_db.insert(28, 'CLOSEDDATE', pd.to_datetime(pandas_db.pop('CLOSEDDATE')))
        pandas_db.insert(29, 'UNDER CONTRACT LENGTH', pandas_db['CLOSEDDATE'] - pandas_db['PENDINGDATE'])
        # List item 5
        pandas_db["POOL"].fillna('N')
        pandas_db["POOLDESC"].fillna('N')
        # List item 6
        street_num = pandas_db.pop('STREETNUMDISPLAY')
        street_add = pandas_db.pop('STREETNAME')
        pandas_db.insert(3, 'ADDRESS', street_num.str.cat(street_add, join='left', sep=' ')
                         .str.replace(r'Rd$', 'Road', regex=True)
                         .str.replace(r'Ct$', 'Court', regex=True)
                         .str.replace(r'St$', 'Street', regex=True)
                         .str.replace(r'Ave$', 'Avenue', regex=True)
                         .str.replace(r'Dr$', 'Drive', regex=True)
                         .str.replace(r'Ln$', 'Lane', regex=True)
                         .str.replace(r'Pl$', 'Place', regex=True)
                         .str.replace(r'Ter$', 'Terrace', regex=True)
                         .str.replace(r'Hwy$', 'Highway', regex=True)
                         .str.replace(r'Pkwy$', 'Parkway', regex=True)
                         .str.replace(r'Cir$', 'Circle', regex=True))
        # List item 7
        pandas_db.insert(3, 'LATITUDE', 0)
        pandas_db.insert(4, 'LONGITUDE', 0)

        return pandas_db

    @staticmethod
    @logger_decorator
    @clean_db_decorator
    def clean_db(**kwargs):
        """
        This function accepts an Excel document or Pandas database to clean and transform all data into uniform
        datatypes before being transferred into a SQL database. This also fortifies the data with all the proper
        living space sq_ft and converts all lot size values to sq_ft
        - Fill the SQFT column using sq_ft_finder method
        :param dirty_db:
        :param tax_db:
        :param property_type:
        :return:
        """

        dirty_db = kwargs['db']
        tax_db = kwargs['tax_db']
        property_type = kwargs['property_type']

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        target_columns = ['TAXID', 'MLSNUM', 'BLOCKID', 'LOTID', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY',
                          'ROOMS', 'BEDS', 'BATHSTOTAL', 'LOTSIZE', 'LOTDESC', 'SQFTAPPROX', 'ORIGLISTPRICE', 'LISTPRICE',
                          'SALESPRICE', 'SPLP', 'LOANTERMS', 'YEARBUILT', 'YEARBUILTDESC', 'STYLEPRIMARY', 'PROPCOLOR',
                          'RENOVATED',  'TAXAMOUNT', 'TAXRATE', 'LISTDATE', 'PENDINGDATE',
                          'CLOSEDDATE', 'DAYSONMARKET', 'OFFICENAME', 'OFFICEPHONE', 'FAX',
                          'AGENTNAME', 'AGENTPHONE', 'COMPBUY', 'SELLOFFICENAME', 'SELLAGENTNAME', 'FIREPLACES',
                          'GARAGECAP', 'POOL', 'POOLDESC', 'BASEMENT', 'BASEDESC', 'AMENITIES', 'APPLIANCES', 'COOLSYSTEM',
                          'DRIVEWAYDESC', 'EXTERIOR', 'FLOORS', 'HEATSRC', 'HEATSYSTEM', 'ROOF',
                          'SIDING', 'SEWER', 'WATER', 'WATERHEATER', 'ROOMLVL1DESC', 'ROOMLVL2DESC', 'ROOMLVL3DESC',
                          'REMARKSPUBLIC']

        if property_type == 'RES':
            clean_db = dirty_db[target_columns].fillna(np.nan)
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_res).pipe(GSMLS.sq_ft_finder, tax_db=tax_db)\
                .pipe(GSMLS.convert_lot_size, property_type=property_type)

        elif property_type == 'MUL':
            temp_target_columns = [column for column in target_columns if column not in ['POOL', 'POOLDESC', 'SQFTAPPROX']]
            temp_target_columns.extend(['UNIT1BATHS', 'UNIT1BEDS', 'UNIT2BATHS', 'UNIT2BEDS', 'UNIT3BATHS', 'UNIT3BEDS',
                                   'UNIT4BATHS', 'UNIT4BEDS'])
            clean_db = dirty_db[temp_target_columns].fillna(np.nan)
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_mul).pipe(GSMLS.total_units)\
                .pipe(GSMLS.convert_lot_size, property_type=property_type)

        elif property_type == 'LND':
            remove_columns = ['ROOMS', 'BEDS', 'BATHSTOTAL', 'YEARBUILT', 'POOLDESC', 'SQFTAPPROX',
                              'YEARBUILTDESC', 'STYLEPRIMARY', 'PROPCOLOR', 'RENOVATED', 'FIREPLACES',
                              'GARAGECAP', 'POOL', 'BASEMENT', 'BASEDESC', 'AMENITIES', 'APPLIANCES', 'COOLSYSTEM',
                              'DRIVEWAYDESC', 'EXTERIOR', 'FLOORS', 'HEATSRC', 'HEATSYSTEM', 'ROOF',
                              'SIDING', 'SEWER', 'WATER', 'WATERHEATER', 'ROOMLVL1DESC', 'ROOMLVL2DESC',
                              'ROOMLVL3DESC', 'POOL']
            temp_target_columns = [column for column in target_columns if column not in remove_columns]
            temp_target_columns.extend(['NUMLOTS', 'ZONING', 'BUILDINGSINCLUDED', 'CURRENTUSE', 'DEVSTATUS', 'DOCSAVAIL',
                                   'EASEMENT', 'FLOODINSUR', 'FLOODZONE', 'IMPROVEMENTS', 'LOCATION',
                                   'PERCTEST', 'ROADSURFACEDESC', 'SERVICES', 'SEWERINFO', 'SOILTYPE', 'WATERINFO',
                                   'ZONINGDESC'])
            clean_db = dirty_db[temp_target_columns].fillna(np.nan)
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_lnd)\
                .pipe(GSMLS.convert_lot_size, property_type=property_type)

        return clean_db

    def comps(self, property_address, br=None, bth=None, sq_ft=None, home_type=None):
        """
        Method which accepts a property address as an expected argument. Other expected agruments with a default
        value of None but if given, can help better narrow the comps.
        I need to be able to animate the GSMLS map tool so I can find all comps within a mile
        Follow the NABPOPs Guidelines for Comparables to ensure the model gives the best comps.
        The following ideas need to be included:
        - Guidelines for comps
        - Lack of comps
        - Market Considerations
        - Rating Property/Amenities
        - Adjustment features
        - Land Value

        TRANSFORM THE DATABASE TO HAVE COLUMN NAMES AS THE INDEX AND THE PROPERTY NAMES AS THE COLUMN NAMES!!!

        :param property_address:
        :param br:
        :param bth:
        :param sq_ft:
        :param home_type:
        :return:
        """

        pass

    @staticmethod
    def convert_lot_size(db, property_type):
        """

        :param db:
        :param property_type:
        :return:
        """

        acres_pattern = r'(\.\d{1,6}?|\d{1,4}?\.\d{1,6}?)\sAC'
        by_pattern = r'(\d{1,5})X\s(\d{1,5})|(\d{1,5})X(\d{1,5})'
        db = db.astype({'LOTSIZE': 'string'})
        lotsize_sqft = db['LOTSIZE']

        if property_type == 'RES':
            db.insert(12, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True))

        elif property_type == 'MUL':
            db.insert(12, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True))

        elif property_type == 'LND':
            db.insert(16, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True))

        return db

    def descriptive_stats_state(self):
        # Run descriptive analysis on all the homes for the state for the quarter
        pass

    def descriptive_stats_county(self):
        # Run descriptive analysis on all the homes for the county for the quarter
        pass

    def descriptive_stats_township(self):
        # Run descriptive analysis on all the homes for the city/town for the quarter
        # Create pie-charts for: types of mortgages used, home types, bed/bath combos, avg beds, avg baths
        # Avg with pools, fireplaces, central air, etc
        pass

    @staticmethod
    def download_manager(cities, city_id, property_type, qtr, driver_var, logger):

        GSMLS.set_city(city_id, driver_var)
        GSMLS.show_results(driver_var)
        time.sleep(2)
        page_results1 = driver_var.page_source
        if "close_generated_popup('alert_popup')" in str(page_results1):
            # No results found
            GSMLS.no_results(city_id, driver_var)
            logger.info(f'There is no GSMLS {property_type} sales data available for {cities[city_id]}')

        else:
            # Results were found
            GSMLS.results_found(driver_var, cities[city_id], qtr, property_type)
            GSMLS.set_city(city_id, driver_var)
            logger.info(f'Sales data for {cities[city_id]} has been downloaded')

    @staticmethod
    def find_cities(page_source):
        """

        :param page_source:
        :return:
        """
        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'title="(\d{4,5}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find('div', {"id": "town1"})
        target_contents = target.find_all('div', {'class': 'selection-item'})
        cities = {}

        for i in target_contents:
            main_contents = str(i)  # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = value_pattern.search(main_contents)
            cities[target_search[1]] = target_search[2]

        return cities

    @staticmethod
    def find_counties(page_source):
        """

        :param page_source:
        :return:
        """
        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'(\d{2})\s-\s(\w+)')
        target_pattern = re.compile(r'title="(\d{2,3}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, 'html.parser')
        target_contents = soup.find_all('label', {'title': value_pattern})
        counties = {}

        for i in target_contents:
            main_contents = str(i)  # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = target_pattern.search(main_contents)
            counties[target_search[1]] = target_search[2]

        return counties

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
        username = db.loc['GSMLS', 'Username']
        pw = db.loc['GSMLS', 'Password']

        os.chdir(previous_wd)

        return username, pw

    def hotsheets(self):
        # Run the Hotsheets on GSMLS to pull the back on market, withdrawn listings, price changes from target cities
        pass

    @logger_decorator
    def lat_long(self, db, county=None, city=None, **kwargs):
        """
        Function used to find a property's latitude and longitude values to calculate the distance from the target
        property
        :param db:
        :param county:
        :param city:
        :param kwargs:
        :return:
        """
        # Fortifies a current df of properties with the longitude and latitude info
        # to be used to calculate the distance between a target property and the comp
        # Use https://www.latlong.net/

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        # counties = NJTaxAssessment.state_county_dictionary()
        good_property_pattern = re.compile(r'(\d{1,5}?-\d{1,5}?|\d{1,5}?)\s(.*)')
        bad_property_pattern = re.compile(r'^[a-zA-Z]')
        url = 'https://geocode.maps.co/search?q='

        try:
            property_address_list = db['ADDRESS'].to_list()
            db = db.set_index('ADDRESS')
            for i in property_address_list:
                if bad_property_pattern.search(i):
                    continue
                elif good_property_pattern.search(i):
                    if (db.loc[i, 'LATITUDE'] or db.loc[i, 'LONGITUDE']) == 0:
                        raw_addr = i
                        address = '+'.join(raw_addr.split(' '))
                        city = db.loc[i, 'TOWN']
                        state = 'NJ'

                        true_url = url + '+'.join([address, city, state])
                        response = requests.get(true_url)  # Be sure to use the proxies in the requests
                        json_results = response.json()

                        if len(json_results) > 1:
                            db.at[i, 'LATITUDE'] = float(json_results[0]['lat'])
                            db.at[i, 'LONGITUDE'] = float(json_results[0]['lon'])
                            time.sleep(1.5)  # Self throttling to not throw the HTTP 429 response
                        else:
                            db.at[i, 'LATITUDE'] = float(json_results['lat'])
                            db.at[i, 'LONGITUDE'] = float(json_results['lon'])
                            time.sleep(1.5)  # Self throttling to not throw the HTTP 429 response
                    else:
                        continue

        # I need to put Request modules exceptions here
        except Exception as e:
            print(e)

        else:
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

            return db

    @staticmethod
    def length_and_width_to_sqft(search_string):
        if search_string.group(1) is not None:
            return str(float(search_string.group(1)) * float(search_string.group(2)))
        else:
            return str(float(search_string.group(3)) * float(search_string.group(4)))
    @staticmethod
    def login(driver_var):
        """

        :param driver_var:
        :return:
        """
        username, pw = GSMLS.get_us_pw(GSMLS)

        gsmls_id = driver_var.find_element(By.ID, 'usernametxt')
        gsmls_id.click()
        gsmls_id.send_keys(username)
        password = driver_var.find_element(By.ID, 'passwordtxt')
        password.click()
        password.send_keys(pw)
        login_button = driver_var.find_element(By.ID, 'login-btn')
        login_button.click()
        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, 'html.parser')
        if 'class="gs-btn-submit-sh gs-btn-submit-two Yes-focus"' in str(soup):
            terminate_duplicate_session = WebDriverWait(driver_var, 5).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="message-box"]/div[3]/input[1]')))
            terminate_duplicate_session.click()
        else:
            pass

    @staticmethod
    def no_results(city_id_var, driver_var):

        no_results_found = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="alert_popup"]/div/div[2]/input')))
        no_results_found.click()
        GSMLS.set_city(city_id_var, driver_var)

    @staticmethod
    def open_run_log():

        previous_dir = os.getcwd()
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Saved Data')
        with shelve.open('GSMLS Run Dictionary', writeback=True) as saved_data_file:
            run_log: dict = saved_data_file['Run Log']

        os.chdir(previous_dir)

        return run_log

    @staticmethod
    def paige_criteria(driver_var):

        uncheck_all = driver_var.find_element(By.ID, "uncheck-all")
        uncheck_all.click()  # Step 2: Uncheck unwanted statuses
        sold_status = driver_var.find_element(By.ID, "S")
        sold_status.click()  # Step 3: Check the sold status

    def paired_sales_analysis(self, city):

        """
        Run a feature valuation or paired sales analysis for features of homes to know what adjustments to make
        when running comparibles
        :param city:
        :return:
        """
        pass

    @staticmethod
    @logger_decorator
    def pandas2sql(**kwargs):
        pass

    def population(self, city):
        # Create a method that can look into the population of a city over
        # a 5/10/30-year period and determine the future growth of the city
        pass

    def possible_mls_deals(self, city):
        # Create a method that can look for deals on the MLS:
        # listings with DOM > X amount of days or under a certain price
        pass

    @staticmethod
    @logger_decorator
    @quarterly_sales
    def quarterly_sales_res(driver_var, county_name=None, city_name=None, **kwargs):
        """
        Method that downloads all the sold homes for each city after each quarter.
        This will help me build a database for all previously
        sold homes to run analysis. Save the name of the file with the city name, the county, quarter, year.
        This initial dataframe will be dirty and have unnecessary information.
        Will be saved to Selenium Temp folder to be cleaned for future use by other methods.

        Will cause ElementClickIntercepted errors if not run on full screen

        :param driver_var:
        :param county_name:
        :param city_name:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']
        run_log: dict = kwargs['Run Log']
        qtr = kwargs['Qtr']
        date_range = kwargs['Dates']
        property_type = 'RES'

        page_results = driver_var.page_source
        # Step 1: Choose the property type for the quick search
        GSMLS.quicksearch(page_results, property_type, driver_var)
        page_check = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'required')))

        if page_check:
            results = driver_var.page_source
            GSMLS.paige_criteria(driver_var)
            GSMLS.res_property_styles(driver_var, results)  # Step 2: Choose target home types
            counties = GSMLS.find_counties(results)  # Step 3: Find all the counties available
            GSMLS.set_dates(date_range, driver_var)  # Step 4: Set the target dates to search for data
            logger.info(f'Results for {qtr} ({date_range[0]} - {date_range[1]}) will now be extracted.')
            run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'IN PROGRESS', logger)

            counties_ids_list = counties.keys()

            if (county_name and city_name) is None:

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:
                        # Step 5: Search for all available municipalities in the target county
                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)
                        cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            # Step 6: Download sales data from all municipalities which has data
                            # If no data is available, continue the program
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)

            elif (county_name and city_name) is not None:
                # Step 3a: There are instances where the program can be terminated due to selenium exceptions
                # One particular exception is the TimeoutException which occurs when an element cant be found
                # This code block allows the program to continue where it left off
                switch_case = 'YES'

                # Modify the county list to start from the county where the program was terminated
                county_index = list(counties.values()).index(county_name)
                counties_ids_list = list(counties.keys())[county_index:]

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:

                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)

                        if switch_case == 'YES':
                            try:
                                # Step 6a:
                                # Modify the city list to start from the city where the program was terminated
                                city_index = list(cities.values()).index(city_name)
                                cities_ids_list = list(cities.keys())[city_index + 1:]
                            except ValueError:
                                cities_ids_list = cities.keys()
                        else:
                            cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)
                        switch_case = 'NO'

        run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'DOWNLOADED', logger)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

        return run_log

    @staticmethod
    @logger_decorator
    @quarterly_sales
    def quarterly_sales_mul(driver_var, county_name=None, city_name=None, **kwargs):
        """
        Method that downloads all the sold multi-family for each city after each quarter.
        This will help me build a database for all previously
        sold homes to run analysis. Save the name of the file with the city name, the county, quarter, year.
        This initial dataframe will be dirty and have unnecessary information. Clean it for future use by other methods.
        Be sure to add columns for longitude and latitude. Be sure to
        fortify the df with the year built, sq_ft, building description,
        etc from the file(s) created by the nj_database method
        :param driver_var:
        :param county_name:
        :param city_name:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']
        run_log: dict = kwargs['Run Log']
        qtr = kwargs['Qtr']
        date_range = kwargs['Dates']
        property_type = 'MUL'

        page_results = driver_var.page_source
        GSMLS.quicksearch(page_results, property_type, driver_var)
        page_check = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'required')))

        if page_check:
            results = driver_var.page_source
            GSMLS.paige_criteria(driver_var)

            counties = GSMLS.find_counties(results)  # Step 1: Find all the counties available
            GSMLS.set_dates(date_range, driver_var)
            logger.info(f'Results for {qtr} ({date_range[0]} - {date_range[1]}) will now be extracted.')
            run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'IN PROGRESS', logger)

            counties_ids_list = counties.keys()

            if (county_name and city_name) is None:

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:

                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)
                        cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)

            elif (county_name and city_name) is not None:

                switch_case = 'YES'

                county_index = list(counties.values()).index(county_name)
                counties_ids_list = list(counties.keys())[county_index:]

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:

                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)

                        if switch_case == 'YES':
                            try:
                                city_index = list(cities.values()).index(city_name)
                                cities_ids_list = list(cities.keys())[city_index + 1:]
                            except ValueError:
                                cities_ids_list = cities.keys()
                        else:
                            cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)
                        switch_case = 'NO'

        run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'DOWNLOADED', logger)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

        return run_log

    @staticmethod
    @logger_decorator
    @quarterly_sales
    def quarterly_sales_lnd(driver_var, county_name=None, city_name=None, **kwargs):
        """
        Method that downloads all the sold land plots for each city after each quarter.
        This will help me build a database for all previously
        sold homes to run analysis. Save the name of the file with the city name, the county, quarter, year.
        This initial dataframe will be dirty and have unnecessary information. Clean it for future use by other methods.
        Be sure to add columns for longitude and latitude. Be sure to
        fortify the df with the year built, sq_ft, building description,
        etc from the file(s) created by the nj_database method
        :param driver_var:
        :param county_name:
        :param city_name:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']
        run_log: dict = kwargs['Run Log']
        qtr = kwargs['Qtr']
        date_range = kwargs['Dates']
        property_type = 'LND'

        page_results = driver_var.page_source
        GSMLS.quicksearch(page_results, property_type, driver_var)
        page_check = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'required')))

        if page_check:
            results = driver_var.page_source
            GSMLS.paige_criteria(driver_var)

            counties = GSMLS.find_counties(results)  # Step 1: Find all the counties available
            GSMLS.set_dates(date_range, driver_var)
            logger.info(f'Results for {qtr} ({date_range[0]} - {date_range[1]}) will now be extracted.')
            run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'IN PROGRESS', logger)

            counties_ids_list = counties.keys()

            if (county_name and city_name) is None:

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:

                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)
                        cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)

            elif (county_name and city_name) is not None:

                switch_case = 'YES'

                county_index = list(counties.values()).index(county_name)
                counties_ids_list = list(counties.keys())[county_index:]

                for county_id in counties_ids_list:
                    if counties[county_id] == 'Other':
                        continue
                    else:

                        cities = GSMLS.cities_download_manager(counties, county_id, driver_var, logger)

                        if switch_case == 'YES':
                            try:
                                city_index = list(cities.values()).index(city_name)
                                cities_ids_list = list(cities.keys())[city_index + 1:]
                            except ValueError:
                                cities_ids_list = cities.keys()
                        else:
                            cities_ids_list = cities.keys()

                        for city_id in cities_ids_list:
                            GSMLS.download_manager(cities, city_id, property_type, qtr, driver_var, logger)

                        logger.info(
                            f'Sales data for municipalities located in {counties[county_id]} County is now complete')
                        GSMLS.set_county(county_id, driver_var)
                        switch_case = 'NO'

        run_log = GSMLS.save_run_log(run_log, qtr, property_type, 'DOWNLOADED', logger)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

        return run_log

    @staticmethod
    def quarterly_appr_depr(county, city, quarter):
        """
        Method which calculates the quarterly neighborhood appreciation/depreciation based on homes gross livable
        area (GLA), homes prices and dates.
        Need to use a minimum of 30 homes minimum. Save this information in the same file as quarterly_sales
        :param county:
        :param city:
        :param quarter:
        :return:
        """

        base_path = 'F\.........'

        quarter_list = ['Q1', 'Q2', 'Q3', 'Q4']

        os.chdir(base_path)
        year = datetime.today().year
        filename = os.path.join(base_path, county, city, city + ' ' + quarter + ' ' + str(year) + ' ' + 'Sales')

        if os.path.exists(filename):
            db1 = pd.read_excel(filename)
            if quarter == 'Q1':
                previous_qtr = 'Q4'
                db2 = pd.read_excel(os.path.join(base_path, county, city,
                                                 city + ' ' + previous_qtr + ' ' + str(year - 1) + ' ' + 'Sales'))
            else:
                db2 = pd.read_excel(os.path.join(base_path, county, city,
                                             city + ' ' + quarter_list[quarter_list.index(quarter) - 1] + ' ' + str(
                                                 year - 1) + ' ' + 'Sales'))
        else:
            raise AttributeError or IOError

            #  Run calculations

    @staticmethod
    def quicksearch(page_results, search_type, driver_var):

        soup = BeautifulSoup(page_results, 'html.parser')
        target = soup.find('li', {"class": "nav-header", "id": "2"})
        submenu_id = target.find('a', {"href": "#", "class": "has-submenu"})['id']
        main_search = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, submenu_id)))
        main_search.click()
        quicksearch_menu = target.find('li', {"id": "2_2"})
        quicksearch_menu_id = quicksearch_menu.find('a', {"href": "#", "class": "disabled has-submenu"})['id']
        # Get the ID of the 2nd submenu
        extended_search_menu = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, quicksearch_menu_id)))
        extended_search_menu.click()

        if search_type == 'RES':
            res_search = WebDriverWait(driver_var, 5).until(
                    EC.presence_of_element_located((By.ID, '2_2_1')))
            res_search.click()

        elif search_type == 'MUL':
            mul_search = WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located((By.ID, '2_2_2')))
            mul_search.click()

        elif search_type == 'LND':
            lnd_search = WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located((By.ID, '2_2_3')))
            lnd_search.click()

    @staticmethod
    def res_property_styles(driver_var, page_source):
        """

        :param driver_var:
        :param page_source:
        :return:
        """
        prop_style_pattern = re.compile(r'title="(.*)"')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find_all('div', {"class": "selection-item"})
        property_style_dict = {}

        for idx, i in enumerate(target[22:]):  # Target[22] is the first instance of property types
            target_contents = str(i)
            prop_style_search = prop_style_pattern.search(target_contents)
            property_style_dict[idx + 1] = prop_style_search.group(1)

        for k in property_style_dict.keys():
            if k in [1, 18, 20, 25, 26, 31, 32, 33, 36, 37, 38, 39, 40, 41, 42]:
                continue
            else:
                selection = driver_var.find_element(By.ID, "selectedStyle" + str(k))
                selection.click()

    @staticmethod
    def results_found(driver_var, city_var, qtr_var, property_type):
        check_all_results = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.ID, 'checkall')))
        check_all_results.click()
        download_results = driver_var.find_element(By.XPATH, '//*[@id="sub-navigation-container"]/div/nav[1]/a[12]')
        download_results.click()
        download_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Download']")))
        excel_file_input = driver_var.find_element(By.ID, 'downloadfiletype3')
        excel_file_input.click()
        filename_input = driver_var.find_element(By.ID, 'filename')
        filename_input.click()
        AC(driver_var).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(
            city_var + ' ' + qtr_var + str(datetime.today().year) + ' ' + property_type + ' Sales GSMLS.xls').perform()
        download_button.click()
        # GSMLS.sort_file()
        close_page = driver_var.find_element(By.XPATH, "//*[@id='sub-navigation-container']/div/nav[1]/a[2]")
        close_page.click()
        close_form = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='sub-navigation-container']/div/nav[1]/a[15]")))
        close_form.click()

    @staticmethod
    def save_run_log(run_log_object, quarter, property_type, status_type, logger, message=None):

        if message is None:
            previous_dir = os.getcwd()
            old_status = run_log_object[property_type][quarter]
            run_log_object[property_type][quarter] = status_type
            os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Saved Data')
            with shelve.open('GSMLS Run Dictionary', writeback=True) as saved_data_file:
                saved_data_file['Run Log'] = run_log_object

            os.chdir(previous_dir)
            logger.info(f'{property_type} {quarter}  status has been changed from {old_status} to {status_type}.'
                        f'Run log has been saved.')
            # print(f'{property_type} {quarter}  status has been changed from {old_status} to {status_type}.'
            #             f'Run log has been saved.')

        else:
            previous_dir = os.getcwd()
            os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Saved Data')
            with shelve.open('GSMLS Run Dictionary', writeback=True) as saved_data_file:
                saved_data_file['Run Log'] = run_log_object

            os.chdir(previous_dir)
            logger.info(f'{message}')

        return run_log_object

    @staticmethod
    def set_city(city_id_var, driver_var):

        click_city = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, city_id_var)))
        click_city.click()

    @staticmethod
    def set_county(county_id_var, driver_var):

        click_county = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, county_id_var)))
        click_county.click()

    @staticmethod
    def set_dates(date_range, driver_var):

        starting_close_date = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, 'closedatemin')))
        starting_close_date.click()  # Step 5: Choose start date
        AC(driver_var).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(date_range[0]).perform()
        ending_close_date = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, 'closedatemax')))
        ending_close_date.click()  # Step 6: Choose end date
        AC(driver_var).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(date_range[1]).perform()

    @staticmethod
    def show_results(driver_var):

        show_results = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'show')))
        show_results.click()

    @staticmethod
    def sign_out(driver_var):

        user = WebDriverWait(driver_var, 5).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="user"]/span[2]')))
        user.click()
        sign_out_button = WebDriverWait(driver_var, 5).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="logout"]')))
        sign_out_button.click()

    # @staticmethod
    # def sort_file(county, city, filename):
    #     """
    #     Will find the recently downloaded zip file of the city
    #     for which all the property information is located. This function will accept the
    #     temporary file name and city as arguments and rename the file with the respective
    #     city in the name and store it in the specific directory under that county
    #     :param county:
    #     :param city:
    #     :param temp_file_name:
    #     :return:
    #     """
    #
    #     previous_dir = os.getcwd()
    #     path = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'
    #     base_path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Property Data'
    #     os.chdir(path)
    #     filenames = os.listdir(path)
    #
    #     try:
    #         for file in filenames:
    #             target_path = os.path.join(base_path, county, city)
    #             if temp_file_name is not None:
    #                 if temp_file_name != file:
    #                     continue
    #
    #                 elif temp_file_name == file:
    #                     extract_file = ZipFile(os.path.abspath(file))
    #                     target_file = temp_file_name.rstrip('.zip') + '.csv'
    #                     extract_file.extract(target_file)
    #                     extract_file.close()
    #
    #                     if os.path.exists(target_path):
    #                         time.sleep(0.5)
    #                         shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
    #                                                                                + 'Database' + ' ' + str(
    #                             datetime.today().date()) + '.csv'))
    #                     else:
    #                         os.makedirs(target_path)
    #                         time.sleep(0.5)
    #                         shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
    #                                                                                + 'Database' + ' ' + str(
    #                             datetime.today().date()) + '.csv'))
    #             elif file.startswith('TaxData'):
    #                 target_file = file + '.xlsx'
    #                 if os.path.exists(target_path):
    #                     time.sleep(0.5)
    #                     shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
    #                                                                            + 'Database' + ' ' + str(
    #                         datetime.today().date()) + '.xlsx'))
    #                 else:
    #                     os.makedirs(target_path)
    #                     time.sleep(0.5)
    #                     shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
    #                                                                            + 'Database' + ' ' + str(
    #                         datetime.today().date()) + '.xlsx'))
    #             # else:
    #             #     if temp_file_name is not None:
    #             #         raise IndexError(f"File Does Not Exist: {temp_file_name}")
    #             #     else:
    #             #         raise IndexError(f"File Does Not Exist", county, city)
    #     except IndexError as IE:
    #         print(f'{IE} ----> {city}')
    #
    #     except Exception as E:
    #         print(f'{E}')
    #
    #     else:
    #         send2trash.send2trash(file)
    #         os.chdir(previous_dir)

    @staticmethod
    def sq_ft_finder(db, tax_db):
        """

        :param db:
        :param tax_db:
        :return:
        """

        # I actually may not have to download the whole db. Set index to Property Location and Just get the SQFT column
        address_list = db['ADDRESS'].to_list()

        db.set_index('ADDRESS', inplace=True, drop=True)
        tax_db.set_index('Property Location', inplace=True, drop=True)
        #  Set index to db2['PROPERTY ADDRESS']

        for address in address_list:
            # May need to switch the 'N/A' to pandas.NAdtype instances
            if db.loc[address, 'SQFTAPPROX'] == 0:
                db.at[address, 'SQFTAPPROX'] = tax_db.loc[address.upper(), 'Sq. Ft.']
            elif db.loc[address, 'SQFTAPPROX'] != 0:
                if db.loc[address, 'SQFTAPPROX'] == tax_db.loc[address.upper(), 'Sq. Ft.']:
                    continue
                elif (tax_db.loc[address.upper(), 'Sq. Ft.'] == 0) and int(db.loc[address, 'SQFTAPPROX']) > 0:
                    continue
                else:
                    db.at[address, 'SQFTAPPROX'] = tax_db.loc[address.upper(), 'Sq. Ft.']

        return db

    @staticmethod
    def total_units(db):

        temp_db = db
        temp_db = temp_db.astype({'UNIT1BATHS': 'string', 'UNIT2BATHS': 'string',
                             'UNIT3BATHS': 'string', 'UNIT4BATHS': 'string'})

        temp_dict = {
            'unit1': temp_db['UNIT1BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace(np.nan, '0'),
            'unit2': temp_db['UNIT2BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace(np.nan, '0'),
            'unit3': temp_db['UNIT3BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace(np.nan, '0'),
            'unit4': temp_db['UNIT4BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace(np.nan, '0')
        }

        temp_db2 = pd.DataFrame(temp_dict)
        temp_db2 = temp_db2.astype({'unit1': 'int64', 'unit2': 'int64', 'unit3': 'int64', 'unit4': 'int64'})

        db['TOTALUNITS'] = temp_db2['unit1'] + temp_db2['unit2'] + temp_db2['unit3'] + temp_db2['unit4']

        db.insert(5, 'TOTALUNITS', db.pop('TOTALUNITS'))

        return db

    def under_contract(self, city=None):
        """
        Checks the inventory under contract in that city and checks the percentage of homes which have
        gone under contract in comparison to what's currently available
        :param city:
        :return:
        """
        pass

    @logger_decorator
    def main(self, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        # save_location1 = 'C:\\Users\\jibreel.q.hameed\\Desktop\\Selenium Temp Folder'
        save_location2 = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'  # May need to be changed
        options = Options()
        # Change this directory to the new one: ('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        s = {"savefile.default_directory": save_location2,
             "download.default_directory": save_location2,
             "download.prompt_for_download": False}
        # options.add_experimental_option("detach", True)
        options.add_experimental_option("prefs", s)
        # options.add_argument("--headless=new")
        driver = webdriver.Edge(service=Service(), options=options)
        driver.maximize_window()
        website = 'https://mls.gsmls.com/member/'
        driver.get(website)
        # results = driver.page_source
        try:
            GSMLS.login(driver)
            GSMLS.quarterly_sales_res(driver)
            # GSMLS.quarterly_sales_mul(driver)
            GSMLS.quarterly_sales_lnd(driver)
            GSMLS.sign_out(driver)

        except TimeoutException as TE:
            logger.warning()  # Find a way to get the function name which experienced the TimeoutException
            return driver

        else:
            return None


if __name__ == '__main__':

    try:
        obj = GSMLS()
        program_results = obj.main()
        # if program_results.isinstance() of a selenium driver:
        # raise TimeoutException and restart program

    except TimeoutException:
        # I need to find a way to restart the program over
        # I'll need to wrap the whole main() function in a try-except block and except the
        # TimeoutException. At exception, receive the driver and sign out
        # Start the function over again
        pass
    else:
        # Send a text message saying the program has been completed and summarize results
        obj.clean_db()
