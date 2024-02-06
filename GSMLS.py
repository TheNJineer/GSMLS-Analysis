import re
import time
import os
from copy import deepcopy
from statistics import mean
import requests
import shelve
import datetime
import traceback
import pandas as pd
import numpy as np
from NJTaxAssessment_v2 import NJTaxAssessment
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import psycopg2
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

            res_db_list = []
            mul_db_list = []
            lnd_db_list = []
            state_data_path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Real Estate Data'
            path = 'C:\\Users\\Omar\\Desktop\\STF'

            os.chdir(state_data_path)
            latest_data = os.listdir(state_data_path)[-1]
            state_db = pd.read_excel(latest_data, sheet_name='All Months')

            os.chdir(path)
            dirty_dbs_list = os.listdir(path)
            main_driver, gsmls_window, rpr_window = GSMLS.open_browser_windows()

            for file in dirty_dbs_list:
                if file.endswith('.xlsx'):
                    db = pd.read_excel(file, engine='openpyxl')
                    city_name = db.loc[0, 'TOWN'].rstrip('*1234567890().')
                    city_name2 = deepcopy(city_name)
                    for ending in ['Town', 'Twp', 'Boro', 'City']:
                        if ending in city_name:
                            city_name = city_name.split(ending)[0].strip()
                    county_name = db.loc[0, 'COUNTY'].rstrip('*')
                    property_type = file.split(' ')[-3]
                    mls_type = file.split(' ')[-1].rstrip('.xlsx')
                    qtr = file.split(' ')[-4][:2]
                    year = int(file.split(' ')[-4][2:])

                    tax_db = NJTaxAssessment.city_database(county_name, city_name)
                    tax_db.set_index('Property Location')

                    kwargs['driver'] = main_driver
                    kwargs['gsmls_window'] = gsmls_window
                    kwargs['rpr_window'] = rpr_window
                    kwargs['initial_db'] = db
                    kwargs['tax_db'] = tax_db
                    kwargs['property_type'] = property_type
                    kwargs['mls_type'] = mls_type
                    kwargs['qtr'] = qtr
                    kwargs['median_sales_price'] = GSMLS.median_sales_price(state_db, city_name2, qtr, year)

                    result = original_function(*args, **kwargs)

                    if property_type == 'RES':
                        res_db_list.append(result)

                    elif property_type == 'MUL':
                        mul_db_list.append(result)

                    elif property_type == 'LND':
                        lnd_db_list.append(result)

                else:
                    continue
            # Separate vars may not be necessary. Just insert the
            # Concatenated dbs once the pandas2sql function is created

            res_main_db = pd.concat(res_db_list)
            mul_main_db = pd.concat(mul_db_list)
            lnd_main_db = pd.concat(lnd_db_list)

            GSMLS.kill_logger(logger_var=kwargs['logger'], file_handler=kwargs['f_handler'], console_handler=kwargs['c_handler'])
        return wrapper

    # @staticmethod
    # def kpi(original_function):
    #     def wrapper(*args, **kwargs):
    #
    #         property_type = str(original_function.__name__)[-3:].upper()
    #         if property_type == 'RES':
    #             kpi_db = GSMLS.potential_farm_area_res()
    #         elif property_type == 'MUL':
    #             pass
    #         elif property_type == 'LND':
    #             pass
    #         elif property_type == 'COM':
    #             pass
    #
    #         kpi_dict = {}
    #         quarterly_sales_data = sql2pandas('RES')
    #         for group, data in kpi:
    #             kpi_dict.setdefault([group[0], {})
    #             kpi_dict[group[0]].setdefault(group[1], 0)
    #             # This currently wont work because the city names from both DB dont match
    #             sales_price = quarterly_sales_data[quarterly_sales_data['TOWN'] == group[1]]['SALESPRICE'].median()
    #             std = quarterly_sales_data[quarterly_sales_data['TOWN'] == group[1]].std()
    #             kpi_dict[group[0]][group[1]] = sales_price - std


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
        return str(round(float(search_string.group(1).rstrip(' AC.')) * 43560, 2))

    @staticmethod
    def address_list_scrape(driver_var, logger_var, mls_number, windows_var: list, **kwargs):
        property_archive = WebDriverWait(driver_var, 20).until(
            EC.presence_of_element_located((By.XPATH, "//span[contains(@class,'fa fa-history fa-lg')]")))
        property_archive.click()
        time.sleep(2)
        windows_list = driver_var.window_handles
        new_window2 = [window for window in windows_list if window not in windows_var][0]
        driver_var.switch_to.window(new_window2)

        time.sleep(2)
        address_list = set()
        page_results3 = driver_var.page_source
        soup = BeautifulSoup(page_results3, 'html.parser')
        # print(soup)
        mls_tables = soup.find_all('table', {"class": "oneline"})

        for table in mls_tables:
            main_table2 = table.find('tbody')
            mls_history = main_table2.find_all('tr')[1]
            address_cell = mls_history.find_all('td')[3]

            address_list.add(address_cell.get_text())

        logger_var.info(f'{len(address_list)} historical addresses have been found for MLS#:{mls_number}')
        driver_var.close()
        driver_var.switch_to.window(windows_var[2])
        driver_var.close()
        driver_var.switch_to.window(windows_var[0])

        return address_list

    @staticmethod
    def address_table_results(page_source_var, mls_number, logger_var):
        try:
            soup = BeautifulSoup(page_source_var, 'html.parser')
            table = soup.find('table', {"class": "df-table nomin mart0", "id": "search-help-table"})
            main_table = table.find('tbody')
            all_rows = main_table.find_all('tr')

            assert len(all_rows) > 0, f'There were no MLS results found. MLS#:{mls_number}'
        except AssertionError as AE:
            logger_var.info(f'{AE}')

            return AE
        else:
            return all_rows

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
    def clean_addresses(search_string):

        target_list = str(search_string.group()).split(' ')
        new_address_list = [i for i in target_list if i != '']

        return ' '.join(new_address_list)

    @staticmethod
    def clean_and_transform_data_lnd(pandas_db, mls, qtr):
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
        :param mls
        :param qtr
        :return:
        """

        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'ORIGLISTPRICE': 'int64', 'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'SPLP': 3})

        # List item 2
        pandas_db['MLS'] = mls
        pandas_db['VARIANCE NEEDED'] = 'N*'  # This is a temporary value which will be changed to either Y or N when the function is created
        pandas_db['QTR'] = qtr
        pandas_db['Z-SCORE'] = (pandas_db['SALESPRICE'] - pandas_db['SALESPRICE'].mean()) / pandas_db[
            'SALESPRICE'].std()
        pandas_db.insert(0, 'MLS', pandas_db.pop('MLS'))
        pandas_db.insert(1, 'QTR', pandas_db.pop('QTR'))
        pandas_db.insert(2, 'LATITUDE', 0)
        pandas_db.insert(3, 'LONGITUDE', 0)
        pandas_db.insert(4, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(5, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(6, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(8, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(9, 'TAXID', pandas_db.pop('TAXID').str.strip('*'))
        pandas_db.insert(10, 'MLSNUM', pandas_db.pop('MLSNUM'))
        pandas_db.insert(11, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))
        pandas_db.insert(12, 'LOTDESC', pandas_db.pop('LOTDESC'))
        pandas_db.insert(13, 'VARIANCE NEEDED', pandas_db.pop('VARIANCE NEEDED'))
        pandas_db.insert(14, 'Z-SCORE', pandas_db.pop('Z-SCORE'))
        pandas_db.insert(15, 'ORIGLISTPRICE', pandas_db.pop('ORIGLISTPRICE'))
        pandas_db.insert(16, 'LISTPRICE', pandas_db.pop('LISTPRICE'))
        pandas_db.insert(17, 'SALESPRICE', pandas_db.pop('SALESPRICE'))
        pandas_db.insert(18, 'SPLP', pandas_db.pop('SPLP'))

        # List item 3
        pandas_db.insert(7, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
        # List item 4 and 8
        pandas_db.insert(20, 'LISTDATE', pd.to_datetime(pandas_db.pop('LISTDATE')))
        pandas_db.insert(21, 'PENDINGDATE', pd.to_datetime(pandas_db.pop('PENDINGDATE')))
        pandas_db.insert(22, 'CLOSEDDATE', pd.to_datetime(pandas_db.pop('CLOSEDDATE')))
        pandas_db.insert(23, 'UNDER CONTRACT LENGTH', pandas_db['CLOSEDDATE'] - pandas_db['PENDINGDATE'])

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
        pandas_db.insert(6, 'ADDRESS', pandas_db.pop('ADDRESS').str.replace(r'.*', GSMLS.clean_addresses, regex=True))

        return pandas_db

    @staticmethod
    def clean_and_transform_data_mul(pandas_db, mls, qtr):
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
        :param mls:
        :param qtr:
        :return:
        """

        pandas_db['RENOVATED'] = pandas_db['RENOVATED'].fillna(0)
        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'RENOVATED': 'int64', 'ORIGLISTPRICE': 'int64',
                                      'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'BATHSTOTAL': 1, 'SPLP': 3})

        # List item 2
        pandas_db['MLS'] = mls
        pandas_db['QTR'] = qtr
        pandas_db['Z-SCORE'] = (pandas_db['SALESPRICE'] - pandas_db['SALESPRICE'].mean()) / pandas_db[
            'SALESPRICE'].std()
        pandas_db.insert(0, 'MLS', pandas_db.pop('MLS'))
        pandas_db.insert(1, 'QTR', pandas_db.pop('QTR'))
        pandas_db.insert(2, 'LATITUDE', 0)
        pandas_db.insert(3, 'LONGITUDE', 0)
        pandas_db.insert(4, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(5, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(6, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(7, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(11, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))
        pandas_db.insert(12, 'LOTDESC', pandas_db.pop('LOTDESC'))
        pandas_db.insert(14, 'Z-SCORE', pandas_db.pop('Z-SCORE'))
        pandas_db.insert(15, 'ORIGLISTPRICE', pandas_db.pop('ORIGLISTPRICE'))
        pandas_db.insert(16, 'LISTPRICE', pandas_db.pop('LISTPRICE'))
        pandas_db.insert(17, 'SALESPRICE', pandas_db.pop('SALESPRICE'))
        pandas_db.insert(18, 'SPLP', pandas_db.pop('SPLP'))

        # List item 3
        pandas_db.insert(7, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
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
        pandas_db.insert(6, 'ADDRESS', pandas_db.pop('ADDRESS').str.replace(r'.*', GSMLS.clean_addresses, regex=True))

        return pandas_db

    @staticmethod
    def clean_and_transform_data_res(pandas_db, mls, qtr, median_sales):
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
        :param mls:
        :param qtr:
        :param median_sales:

        :return:
        """

        pandas_db['SQFTAPPROX'] = pandas_db['SQFTAPPROX'].fillna(0)
        pandas_db['RENOVATED'] = pandas_db['RENOVATED'].fillna(0)
        pandas_db = pandas_db.astype({'STREETNUMDISPLAY': 'string', 'STREETNAME': 'string',
                                      'SQFTAPPROX': 'int64', 'RENOVATED': 'int64', 'ORIGLISTPRICE': 'int64',
                                      'LISTPRICE': 'int64', 'SALESPRICE': 'int64'})
        pandas_db.round({'BATHSTOTAL': 1, 'SPLP': 3})

        # List item 2
        pandas_db['MLS'] = mls
        pandas_db['QTR'] = qtr
        pandas_db['Z-SCORE'] = (pandas_db['SALESPRICE'] - median_sales[0]) / median_sales[1]
        pandas_db.insert(0, 'MLS', pandas_db.pop('MLS'))
        pandas_db.insert(1, 'QTR', pandas_db.pop('QTR'))
        pandas_db.insert(2, 'LATITUDE', 0)
        pandas_db.insert(3, 'LONGITUDE', 0)
        pandas_db.insert(4, 'BLOCKID', pandas_db.pop('BLOCKID').str.strip('*'))
        pandas_db.insert(5, 'LOTID', pandas_db.pop('LOTID').str.strip('*'))
        pandas_db.insert(6, 'STREETNAME', pandas_db.pop('STREETNAME').str.strip('*'))
        pandas_db.insert(7, 'COUNTY', pandas_db.pop('COUNTY').str.strip('*'))
        pandas_db.insert(11, 'SQFTAPPROX', pandas_db.pop('SQFTAPPROX'))
        pandas_db.insert(13, 'LOTSIZE', pandas_db.pop('LOTSIZE').str.strip('*'))
        pandas_db.insert(14, 'LOTDESC', pandas_db.pop('LOTDESC'))
        pandas_db.insert(18, 'Z-SCORE', pandas_db.pop('Z-SCORE'))

        # List item 3
        pandas_db.insert(7, 'TOWN', pandas_db.pop('TOWN').str.rstrip('*(1234567890)'))
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
                         .str.replace(r'Rd$|Rd\.$', 'Road', regex=True)
                         .str.replace(r'Ct$|Ct\.$', 'Court', regex=True)
                         .str.replace(r'St$|St\.$', 'Street', regex=True)
                         .str.replace(r'Ave$|Ave\.$', 'Avenue', regex=True)
                         .str.replace(r'Dr$|Dr\.$', 'Drive', regex=True)
                         .str.replace(r'Ln$|Ln\.$', 'Lane', regex=True)
                         .str.replace(r'Pl$|Pl\.$', 'Place', regex=True)
                         .str.replace(r'Ter$|Ter\.$', 'Terrace', regex=True)
                         .str.replace(r'Hwy$|Hwy\.$', 'Highway', regex=True)
                         .str.replace(r'Pkwy$|Pkwy\.$', 'Parkway', regex=True)
                         .str.replace(r'Cir$|Cir\.$', 'Circle', regex=True))
        pandas_db.insert(6, 'ADDRESS', pandas_db.pop('ADDRESS').str.replace(r'.*', GSMLS.clean_addresses, regex=True))

        return pandas_db

    @staticmethod
    @logger_decorator
    @clean_db_decorator
    def clean_db(**kwargs):
        """
        This function accepts an Excel document or Pandas database to clean and transform all data into uniform
        datatypes before being transferred into a SQL database. This also fortifies the data with all the proper
        living space sq_ft and converts all lot size values to sq_ft
        - Fill the SQFT column using find_sq_ft method
        :param dirty_db:
        :param tax_db:
        :param property_type:
        :param qtr:
        :return:
        """

        dirty_db = kwargs['initial_db']
        # tax_db = kwargs['tax_db']
        property_type = kwargs['property_type']
        mls_type = kwargs['mls_type']
        qtr = kwargs['qtr']
        median_sales_prices = kwargs['median_sales_price']

        target_columns = ['TAXID', 'MLSNUM', 'BLOCKID', 'LOTID', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY',
                          'ROOMS', 'BEDS', 'BATHSTOTAL', 'LOTSIZE', 'LOTDESC', 'SQFTAPPROX', 'ORIGLISTPRICE', 'LISTPRICE',
                          'SALESPRICE', 'SPLP', 'LOANTERMS', 'YEARBUILT', 'YEARBUILTDESC', 'STYLEPRIMARY',
                          'PROPCOLOR', 'RENOVATED',  'TAXAMOUNT', 'TAXRATE', 'LISTDATE', 'PENDINGDATE',
                          'CLOSEDDATE', 'DAYSONMARKET', 'OFFICENAME', 'OFFICEPHONE', 'FAX',
                          'AGENTNAME', 'AGENTPHONE', 'COMPBUY', 'SELLOFFICENAME', 'SELLAGENTNAME', 'FIREPLACES',
                          'GARAGECAP', 'POOL', 'POOLDESC', 'BASEMENT', 'BASEDESC', 'AMENITIES', 'APPLIANCES', 'COOLSYSTEM',
                          'DRIVEWAYDESC', 'EXTERIOR', 'FLOORS', 'HEATSRC', 'HEATSYSTEM', 'ROOF',
                          'SIDING', 'SEWER', 'WATER', 'WATERHEATER', 'ROOMLVL1DESC', 'ROOMLVL2DESC', 'ROOMLVL3DESC',
                          'REMARKSPUBLIC']

        if property_type == 'RES':
            clean_db = dirty_db[target_columns].fillna(np.nan)
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_res, mls=mls_type, qtr=qtr, median_sales=median_sales_prices)\
                .pipe(GSMLS.find_sq_ft, **kwargs)\
                .pipe(GSMLS.convert_lot_size, property_type=property_type)

        elif property_type == 'MUL':
            temp_target_columns = [column for column in target_columns if column not in ['POOL', 'POOLDESC',
                                    'SQFTAPPROX', 'STYLEPRIMARY', 'FIREPLACES', 'AMENITIES', 'APPLIANCES',
                                    'FLOORS', 'ROOMLVL1DESC', 'ROOMLVL2DESC', 'ROOMLVL3DESC']]
            temp_target_columns.extend(['UNIT1BATHS', 'UNIT1BEDS', 'UNIT2BATHS', 'UNIT2BEDS', 'UNIT3BATHS', 'UNIT3BEDS',
                                   'UNIT4BATHS', 'UNIT4BEDS'])
            clean_db = dirty_db[temp_target_columns].fillna(np.nan)
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_mul, mls=mls_type, qtr=qtr)\
                .pipe(GSMLS.total_units).pipe(GSMLS.convert_lot_size, property_type=property_type)

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
            clean_db = clean_db.pipe(GSMLS.clean_and_transform_data_lnd, mls=mls_type, qtr=qtr)\
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
    def connect2postgresql():

        # Do I create a function which retrieve my info from UniversalFunction.get_us_pw?
        '''
        database: the name of the database that you want to connect.
        user: the username used to authenticate.
        password: password used to authenticate.
        host: database server address e.g., localhost or an IP address.
        port: the port number that defaults to 5432 if it is not provided.
        '''

        username, pw = GSMLS.get_us_pw('PostgreSQL')

        conn = psycopg2.connect(
            host="localhost",
            database="nj_realestate_data",
            user=username,
            password=pw)

        cur = conn.cursor()

        return tuple([cur, conn, username, pw])

    @staticmethod
    def convert_lot_size(db, property_type):
        """

        :param db:
        :param property_type:
        :return:
        """

        acres_pattern = r'(\.\d{1,6}(\sAC)?|\.\d{1,6}(\sAC.)?|\d{1,4}\.\d{1,6}(\sAC)?|\d{1,4}\.\d{1,6}(\sAC.)?)'
        by_pattern = r'(\d{1,5})X\s(\d{1,5})|(\d{1,5})X(\d{1,5})|(\d{1,5})\sX\s(\d{1,5})|(\d{1,5})\sX(\d{1,5})'
        db = db.astype({'LOTSIZE': 'string'})
        lotsize_sqft = db['LOTSIZE']

        if property_type == 'RES':
            db.insert(12, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True).str.rstrip('.')
                      .str.replace(',', ''))

        elif property_type == 'MUL':
            db.insert(12, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True).str.rstrip('.')
                      .str.replace(',', ''))

        elif property_type == 'LND':
            db.insert(16, 'LOTSIZE (SQFT)', lotsize_sqft.str.replace(acres_pattern, GSMLS.acres_to_sqft, regex=True)
                      .str.replace(by_pattern, GSMLS.length_and_width_to_sqft, regex=True).str.rstrip('.')
                      .str.replace(',', ''))

        db = db.astype({'LOTSIZE (SQFT)': 'float64'})
        db = db.round({'LOTSIZE (SQFT)': 2})

        return db

    @staticmethod
    def cooling_system_statistics(db, **kwargs):
        # Can only be used with RES and MUL property types
        property_type = str(db.__name__)[-3:].upper()

        if property_type == 'RES' or 'MUL':
            interior_columns = ['COUNTY', 'TOWN', 'COOLSYSTEM']
            interior_df = db[interior_columns].groupby(['COUNTY', 'TOWN'])
            cooling_system_stats = interior_df['COOLSYSTEM'].value_counts()

            return cooling_system_stats
        else:
            raise ValueError

    @staticmethod
    def create_lnd_sales_table(cursor_var, conn_var):

        statement = "CREATE TABLE mul_sales_data (id serial, mls varchar(20), quarter char(2), latitude numeric," \
                    "longitude numeric, blockid smallint, lotid smallint, address varchar(100), town varchar(100)," \
                    "county varchar(100), tax_id varchar(100), mlsnum real, lotsize varchar(50), lotsize_sqft real," \
                    "lot_desc varchar(50), variance_needed varchar(3), z_score real, origlistprice integer," \
                    "listprice integer, salesprice integer, splp real, listdate date, pendingdate date," \
                    "closeddate date, under_contract_length interval, loan_terms varchar(50), tax_amount integer," \
                    "tax_rate real, dom smallint, lotsize varchar(50), office_name varchar(250), office_phone varchar(15)," \
                    "fax varchar(15), listing_agent varchar(100), agent_phone varchar(15), comp_buy varchar(20)," \
                    "buying_office varchar(250), buying_agent varchar(100), public_remarks text, num_lots smallint," \
                    "zoning varchar(50), buildings_included varchar(50), current_use varchar(100)," \
                    "development_status varchar(100), docs_avaialable varchar(100), easement varchar(5)," \
                    "flood_insurance varchar(10), flood_zone varchar(10), improvements varchar(100)," \
                    "location varchar(100), perc_test varchar(100), road_surface_desc varchar(100)," \
                    "services varchar(100), sewer varchar(100) soil_type varchar(100), water_info varchar(100)," \
                    "zoning varchar(100));"

        cursor_var.execute(statement)
        conn_var.commit()

    @staticmethod
    def create_mul_sales_table(cursor_var, conn_var):

        statement = "CREATE TABLE mul_sales_data (id serial, mls varchar(20), quarter char(2), latitude numeric," \
                    "longitude numeric, blockid smallint, lotid smallint, total_units smallint, address varchar(100), town varchar(100)," \
                    "county varchar(100), tax_id varchar(100), mlsnum real, sqft_approx smallint," \
                    "lotsize varchar(50), lotsize_sqft real, rooms smallint, beds smallint, bathstotal real," \
                    "lot_desc varchar(50), z_score real, origlistprice integer, listprice integer, salesprice integer," \
                    "splp real, loan_terms varchar(50), yearbuilt smallint, yearbuilt_desc varchar(20)," \
                    "listdate date, pending_date date, closeddate date, under_contract_length interval, dom smallint," \
                    "primary_style varchar(100), property_color varchar(20), renovated smallint, tax_amount integer," \
                    "tax_rate real, office_name varchar(250), office_phone varchar(15), fax varchar(15)," \
                    "listing_agent varchar(100), agent_phone varchar(15), comp_buy varchar(20)," \
                    "buying_office varchar(250), buying_agent varchar(100), basement char(1), basement_desc varchar(100)," \
                    "coolsystem varchar(100), driveway_desc varchar(100), exterior varchar(100)," \
                    "heatsource varchar(50), heatsystem varchar(100), roof varchar(100)," \
                    "siding varchar(50), sewer varchar(10), water varchar(10), waterheater varchar(10)," \
                    "unit1_baths real, unit1_beds smallint, unit2_baths real, unit2_beds smallint," \
                    "unit3_baths real, unit3_beds smallint, unit4_baths real, unit4_beds smallint," \
                    "public_remarks text);"

        cursor_var.execute(statement)
        conn_var.commit()

    @staticmethod
    def create_res_sales_table(cursor_var, conn_var):

        statement = "CREATE TABLE res_sales_data (id serial, mls varchar(20), quarter char(2), latitude numeric," \
                    "longitude numeric, blockid smallint, lotid smallint, address varchar(100), town varchar(100)," \
                    "county varchar(100), tax_id varchar(100), mlsnum real, sqft_approx smallint," \
                    "lotsize varchar(50), lotsize_sqft real, rooms smallint, beds smallint, bathstotal real," \
                    "lot_desc varchar(50), z_score real, origlistprice integer, listprice integer, salesprice integer," \
                    "splp real, loan_terms varchar(50), yearbuilt smallint, yearbuilt_desc varchar(20)," \
                    "listdate date, pending_date date, closeddate date, under_contract_length interval, dom smallint," \
                    "primary_style varchar(100), property_color varchar(20), renovated smallint, tax_amount integer," \
                    "tax_rate real, office_name varchar(250), office_phone varchar(15), fax varchar(15)," \
                    "listing_agent varchar(100), agent_phone varchar(15), comp_buy varchar(20)," \
                    "buying_office varchar(250), buying_agent varchar(100), fireplaces smallint, garagecap smallint, " \
                    "pool char(1), pooldesc varchar(50), basement char(1), basement_desc varchar(100), ammenities varchar(50)," \
                    "appliances varchar(250), coolsystem varchar(100), driveway_desc varchar(100), exterior varchar(100)," \
                    "floors varchar(100), heatsource varchar(50), heatsystem varchar(100), roof varchar(100), " \
                    "siding varchar(50), sewer varchar(10), water varchar(10), waterheater varchar(10), interior varchar(250)," \
                    "roomlvl1desc text, roomlvl2desc text, roomlvl3desc text, public_remarks text);" \

        cursor_var.execute(statement)
        conn_var.commit()

    @staticmethod
    def create_sql_table(table_name, cursor_var, conn_var, **kwargs):

        logger = kwargs['logger']

        if table_name == 'res_sales_data':
            GSMLS.create_res_sales_table(cursor_var, conn_var)
            logger.info(f'PostgreSQL table named {table_name} has been created')

        elif table_name == 'mul_sales_data':
            GSMLS.create_mul_sales_table(cursor_var, conn_var)
            logger.info(f'PostgreSQL table named {table_name} has been created')

        elif table_name == 'lnd_sales_data':
            GSMLS.create_res_sales_table(cursor_var, conn_var)
            logger.info(f'PostgreSQL table named {table_name} has been created')

        else:
            # placeholder for a block that a user can create a table on spot?
            pass

    def descriptive_stats_state(self):
        # Run descriptive analysis on all the homes for the state for the quarter
        pass

    @staticmethod
    def descriptive_statistics_county(db, **kwargs):
        # Can be used with any property type RES, MUL, LND

        main_columns = ['COUNTY', 'TOWN', 'LISTPRICE', 'SALESPRICE', 'DAYSONMARKET', 'UC-DAYS', 'SPLP']
        target_df = db[main_columns].groupby(['COUNTY', 'TOWN'])
        descriptive_stats = target_df.describe()

        return descriptive_stats

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
    @logger_decorator
    # @kpi
    def email_campaign(**kwargs):
        pass

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
    def find_sq_ft(db, tax_db, **kwargs):
        """

        :param db:
        :param tax_db:
        :return:
        """

        numbers_dict = {'1ST': 'FIRST', 'FIRST': '1ST', '2ND': 'SECOND', 'SECOND': '2ND',
                        '3RD': 'THIRD', 'THIRD': '3RD', '4TH': 'FOURTH', 'FOURTH': '4TH',
                        '5TH': 'FIFTH', 'FIFTH': '5TH', '6TH': 'SIXTH', 'SIXTH': '6TH',
                        '7TH': 'SEVENTH', 'SEVENTH': '7TH', '8TH': 'EIGHTH', 'EIGHTH': '8TH',
                        '9TH': 'NINTH', 'NINTH': '9TH', '10TH': 'TENTH', 'TENTH': '10TH'}

        address_list = db['ADDRESS'].to_list()
        tax_address_list = tax_db['Property Location'].to_list()

        db.set_index('ADDRESS', inplace=True, drop=True)
        tax_db.set_index('Property Location', inplace=True, drop=False)  # Column would still need to be indexed in the event of a ValueError so leave duplicate
        numbered_blocks = re.compile(
            r'\d{1,2}?st|\d{1,2}?nd|\d{1,2}?rd|\d{1,2}?th|First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Nineth|Tenth')

        for address in address_list:
            search_address = numbered_blocks.search(address, re.IGNORECASE)
            if search_address is None:

                db = GSMLS.sq_ft_search(address, db, tax_db, tax_address_list, **kwargs)

            elif search_address is not None:
                # Check if the current spelling of the address is the same as in the tax_tb
                if address.upper() in tax_address_list:

                    db = GSMLS.sq_ft_search(address, db, tax_db, tax_address_list, **kwargs)

                else:
                    # Change the spelling of the address and check tax_db again
                    address_2 = address.replace(search_address.group(0), numbers_dict[search_address.group(0).upper()])

                    db = GSMLS.sq_ft_search(address, db, tax_db, tax_address_list, address_2, **kwargs)

        return db

    @staticmethod
    def floor_type_statistics(db, **kwargs):
        # Can only be used with RES property type

        property_type = str(db.__name__)[-3:].upper()

        if property_type == 'RES':
            type_columns = ['COUNTY', 'TOWN', 'FLOORS']
            homes_df = db[type_columns].groupby(['COUNTY', 'TOWN'])
            floor_type_stats = homes_df.value_counts()

            return floor_type_stats
        else:
            raise ValueError

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

        os.chdir(previous_wd)

        return username, pw

    @staticmethod
    def home_type_statistics(db, **kwargs):
        # Can only be used with RES property type

        property_type = str(db.__name__)[-3:].upper()

        if property_type == 'RES':
            type_columns = ['COUNTY', 'TOWN', 'STYLEPRIMARY']
            homes_df = db[type_columns].groupby(['COUNTY', 'TOWN'])
            home_type_stats = homes_df.value_counts()

            return home_type_stats
        else:
            raise ValueError

    def hotsheets(self):
        # Run the Hotsheets on GSMLS to pull the back on market, withdrawn listings, price changes from target cities
        pass

    @staticmethod
    def kill_logger(logger_var, file_handler, console_handler):

        logger_var.removeHandler(file_handler)
        logger_var.removeHandler(console_handler)
        logging.shutdown()

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
        elif search_string.group(3) is not None:
            return str(float(search_string.group(3)) * float(search_string.group(4)))
        elif search_string.group(5) is not None:
            return str(float(search_string.group(5)) * float(search_string.group(6)))
        elif search_string.group(7) is not None:
            return str(float(search_string.group(7)) * float(search_string.group(8)))

    @staticmethod
    def loan_type_statistics(db, **kwargs):
        # Can be used with any property type RES, MUL, LND

        loan_columns = ['COUNTY', 'TOWN', 'LOANTERMS']
        loan_df = db[loan_columns].groupby(['COUNTY', 'TOWN'])
        loan_stats = loan_df.value_counts()

        return loan_stats

    @staticmethod
    def login(website, driver_var):
        """

        :param website:
        :param driver_var:
        :return:
        """
        username, pw = GSMLS.get_us_pw(website)

        if website == 'GSMLS':
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

        elif website == 'RPR':
            enter_email = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "SignInEmail")))
            enter_email.click()
            enter_email.send_keys(username)

            password = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "SignInPassword")))
            password.click()
            password.send_keys(pw)

            time.sleep(1)  # Built in latency for sign in button to appear. Is grey out until forms are filled
            sign_in = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "SignInBtn")))
            sign_in.click()

    @staticmethod
    def median_sales_price(db, town_name, qtr, year):

        towns_list = db['City'].unique().tolist()
        for instance in towns_list:
            if town_name in instance:
                city = towns_list[towns_list.index(instance)]
                break

        target_db = db[(db['City'] == city) & (db['Quarter'] == qtr) & (db['Year'] == year)]

        mean_sales_price = round(target_db['Median Sales Prices'].mean(), 2)
        sales_prices_std = round(target_db['Median Sales Prices'].std(), 2)

        return tuple([mean_sales_price, sales_prices_std])

    @staticmethod
    def municipality_sales_avg(series, db, dictionary):

        county_list = series.index.to_list()
        county_sales = series.to_list()

        city_sales_per_list = []
        for county, county_sum in zip(county_list, county_sales):
            for group, data in db:
                if county == group[0]:
                    city_sales_per_list.append(data.sum() / county_sum)
                else:
                    continue

        city_sales_avg = round((mean(city_sales_per_list)) * 100, 3)

        market_dict = {}
        for group, data in db:
            if group[0] in dictionary.keys():
                if (data.sum() / dictionary[group[0]][0]) * 100 >= city_sales_avg:
                    market_dict.setdefault(group[0], {})
                    market_dict[group[0]].setdefault(group[1], 0)
                    market_dict[group[0]][group[1]] = round((data.sum() / dictionary[group[0]][0]) * 100, 3)

        return market_dict


    @staticmethod
    def no_results(city_id_var, driver_var):

        no_results_found = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="alert_popup"]/div/div[2]/input')))
        no_results_found.click()
        GSMLS.set_city(city_id_var, driver_var)

    @staticmethod
    def open_browser_windows():
        options = Options()
        # options.add_argument("--headless=new")
        driver = webdriver.Edge(service=Service(), options=options)
        driver.maximize_window()
        website1 = 'https://mls.gsmls.com/member/'
        website2 = 'https://www.narrpr.com/'
        driver.get(website1)

        # Login to the GSMLS
        GSMLS.login('GSMLS', driver)
        gsmls_window = driver.current_window_handle
        # Create a new tab to also log into RPR so it's ready for use
        driver.switch_to.new_window('tab')
        driver.get(website2)
        GSMLS.login('RPR', driver)
        rpr_window = driver.current_window_handle
        # Switch back to the GSMLS window as this window is the first one used in the sq_ft_search()
        driver.switch_to.window(gsmls_window)

        return tuple([driver, gsmls_window, rpr_window])

    @staticmethod
    def open_property_listing(driver_var, list_of_windows, contact_num):

        click_mls_number = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, 'selcontact' + str(contact_num))))
        click_mls_number.click()

        #  Clicking the MLS number will open up a new window. I need to switch to that window
        time.sleep(2)
        windows_list = driver_var.window_handles
        new_window = [window for window in windows_list if window not in list_of_windows][0]
        driver_var.switch_to.window(new_window)

        time.sleep(2)
        page_results2 = driver_var.page_source
        soup = BeautifulSoup(page_results2, 'html.parser')
        sidebar_table = soup.find('div', {"class": "side-bar-padding"})
        sidebar_buttons = sidebar_table.find_all('div', {"class": "sidebar-button select"})

        return tuple([new_window, sidebar_buttons])

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
    def pandas2sql(db, table_name, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        # I need to not drop the duplicate 'ADDRESS' table in the dbs
        cursor, conn, username, pw = GSMLS.connect2postgresql()  # Creates a seperate connection to PostgreSQL

        logger.info('PostgreSQL Database connection made')

        # Creates a connection from Pandas to PostgreSQL
        engine = create_engine(f"postgresql://{username}:{pw}@localhost:5433/nj_realestate_data")

        if GSMLS.sql_table_check(cursor, table_name):
            db.to_sql(table_name, engine, if_exists='append', chunksize=1000, index=False)

        else:
            GSMLS.create_sql_table(table_name, cursor, conn, **kwargs)
            db.to_sql(table_name, engine, if_exists='fail', chunksize=1000, index=False)

        conn.close()

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

    def population(self, city):
        # Create a method that can look into the population of a city over
        # a 5/10/30-year period and determine the future growth of the city
        pass

    @staticmethod
    def potential_farm_area_res(year_var: int, quarter_var: str, median_sales_cap=3000000):
        # Upload the most recent NJRealtor data

        previous_cwd = os.getcwd()
        path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Real Estate Data'
        os.chdir(path)
        latest_file = os.listdir(path)[-1]
        # Use the function which connects to PostgreSQL and gets the most recent table

        db = pd.read_excel(latest_file, sheet_name='All Months')
        os.chdir(previous_cwd)

        temp_df1 = db[(db['Median Sales Prices'] <= median_sales_cap) & (db['Year'] == year_var) & (db['Quarter'] == quarter_var)].sort_values(by=['Closed Sales'], ascending=False)
        target_df = temp_df1.groupby(['County', 'City'])['Closed Sales']
        target_series = temp_df1.groupby('County')['Closed Sales'].sum()

        county_farm_dict = GSMLS.target_counties_res(target_series)
        farm_area = GSMLS.municipality_sales_avg(target_series, target_df, county_farm_dict)

        return farm_area

    @staticmethod
    def property_archive(mls_number, mls_address=None, **kwargs):

        logger = kwargs['logger']
        driver = kwargs['driver']
        gsmls_window = kwargs['gsmls_window']
        rpr_window = kwargs['rpr_window']
        open_window_list = [gsmls_window, rpr_window]

        contact_num = 2

        # Type in the MLS number or address. Create own function that's able to use both if necessary
        page_results = GSMLS.search_listing(mls_number, driver, logger)

        # Find the address table
        all_rows = GSMLS.address_table_results(page_results, mls_number, logger)

        if type(all_rows) is list:
            for row in all_rows:
                if row.td.a['value'] == mls_number:

                    new_window, sidebar_buttons = GSMLS.open_property_listing(driver, open_window_list, contact_num)
                    open_window_list.append(new_window)
                    try:
                        for button in sidebar_buttons:

                            if button.span['class'][1] == 'fa-history':

                                return GSMLS.address_list_scrape(driver, logger, mls_number, open_window_list)

                            elif (button.span['class'][1] != 'fa-history') and (button != sidebar_buttons[-1]):
                                continue

                    except Exception as E:
                        logger.warning(f'{E}')
                        # There's no Property Archive. Use RPR program
                        return f'No historical addresses available for MLS#:{mls_number}'

                else:
                    contact_num += 1
                    continue
        elif type(all_rows) is str:
            return all_rows


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
    def rename_pandas_columns(db):

        # db = db.rename(columns={'City': 'city', 'Dates': 'dates', 'County': 'county',
        #                         'Quarter': 'quarter', 'Month': 'month', 'Year': 'year',
        #                         'New Listings': 'new_listings', 'New Listing % Change (YoY)': 'new_listing_yoy_change',
        #                         'Closed Sales': 'closed_sales', 'Closed Sale % Change (YoY)': 'closed_sales_yoy_change',
        #                         'Days on Markets': 'dom', 'Days on Market % Change (YoY)': 'dom_yoy_change',
        #                         'Median Sales Prices': 'median_sales_price',
        #                         'Median Sales Price % Change (YoY)': 'median_sales_price_yoy_change',
        #                         'Percent of Listing Price Received': 'polpr',
        #                         'Percent of Listing Price Receive % Change (YoY)': 'polpr_yoy_change',
        #                         'Inventory of Homes for Sales': 'inventory_of_homes',
        #                         'Inventory of Homes for Sale % Change (YoY)': 'inventory_of_homes_yoy_change',
        #                         'Months of Supply': 'months_of_supply',
        #                         'Months of Supplies % Change (YoY)': 'months_of_supply_yoy_change'})

        return db

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
    def rpr(search_type, full_address, **kwargs):

        driver = kwargs['driver']
        gsmls_window = kwargs['gsmls_window']
        rpr_window = kwargs['rpr_window']

        driver.switch_to.window(rpr_window)
        address_found = GSMLS.rpr_search_address(full_address, driver)

        if address_found:
            if search_type == 'SQFT':
                results = int(GSMLS.rpr_sq_ft(driver))

            elif search_type == 'FULL':
                results = GSMLS.rpr_property_facts(driver)

            return results

        else:
            return 0

    @staticmethod
    @logger_decorator
    def rpr_property_facts(driver_var, **kwargs):

        county_html_pattern = re.compile(r'<span\s_ngcontent-ng-.*\sclass="ng-tns-.*">(\w+\sCounty)</span>')
        block_lot_pattern = re.compile(r'<span\s_ngcontent-ng-.*\sclass="ng-tns-.*">(LOT:\d{1,5}?\sBLK:\d{1,5}?\sDIST:\d{1,5}?)')

        property_dict = {}
        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, 'html.parser')

        property_dict['estimated_value'] = soup.find('div', {"class": "price ng-star-inserted"}).a.get_text()
        property_dict['rvm_last_updated'] = soup.find('div', {
            "class": "footer ng-star-inserted"}).div.div.next_sibling.get_text()
        property_dict['County'] = county_html_pattern.search(str(soup)).group(1)
        property_dict['Block & Lot'] = block_lot_pattern.search(str(soup)).group(1)

        key_facts = soup.find('section', {"class": "key-facts"})
        basic_facts = soup.find('ul', {"class": "has-columns three-columns ng-star-inserted"})
        property_facts = soup.find('table', {"class": "table is-fullwidth is striped details-table"}).tbody
        exterior_features = soup.find_all('ul', {"class": "flex-item-equal-width ng-star-inserted"})

        for idx, item in enumerate(key_facts.find_all('div')):
            if idx == 0:
                property_dict['sq_ft'] = item.get_text()
            elif idx == 1:
                property_dict['lot_size'] = item.get_text()

        for idx1, item1 in enumerate(basic_facts.find_all('div', {"class": "break-word"})):
            if idx1 == 0:
                property_dict['type'] = item1.get_text()
            elif idx1 == 2:
                property_dict['owner_name'] = item1.get_text()

        for idx2, item2 in enumerate(property_facts.find_all('tr', {"class": "ng-star-inserted"})):
            if idx2 in [2, 3, 4, 5]:
                property_dict[item2.td.get_text().strip()] = item2.find('div', {
                    "class": "is-print-only ng-star-inserted"}).get_text()
            else:
                property_dict[item2.td.get_text().strip()] = item2.td.next_sibling.div.get_text()

        for idx3, item3 in enumerate(exterior_features):
            for idx4, item4 in enumerate(item3.find_all('li', {"class": "ng-star-inserted"})):
                if idx3 == 0 and idx4 == 1:
                    property_dict[item4.div.get_text().strip()] = item4.div.next_sibling.span.get_text().strip()

                elif idx3 == 1 and idx4 == 0:
                    property_dict[item4.div.get_text().strip()] = item4.div.next_sibling.span.get_text().strip()

    # I need tro see how I can get the county, block and lot number

        return property_dict

    @staticmethod
    @logger_decorator
    def rpr_search_address(mls_address, driver_var, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        time.sleep(1.5)  # Built in latency for sign in page to appear.

        main_page_results = driver_var.page_source
        if '<div class="appcues-skip">' in main_page_results:
            close_button = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, "//html/body/appcues/modal-container/div/a")))
            close_button.click()

        location = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter Address, Place, APN/Tax IDs or Listing IDs']")))
        location.click()
        location.send_keys(mls_address)

        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, 'html.parser')
        address_matches = soup.find('div', {"class": "ng-star-inserted"})
        suggested_address_list = address_matches.find_all('div', {
            "class": "suggestion-container auto-suggest-item keyboard-nav-suggestion-item ng-star-inserted"})

        if len(suggested_address_list) < 1:
            logger.info(f'{mls_address} not found')

            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

            return False

        else:
            address_part_list = []
            for idx, address in enumerate(suggested_address_list):

                suggested_text = address.div.find_all('b', {"class": "highlighted-text"})

                for idx1 in suggested_text:
                    address_part_list.append(idx1.get_text())

                true_address = ' '.join(address_part_list)

                if true_address == mls_address:
                    logger.info(f'{mls_address} found')
                    click_address = WebDriverWait(driver_var, 10).until(
                        EC.presence_of_element_located((By.XPATH, "(//div[@class='suggestion-container auto-suggest-item keyboard-nav-suggestion-item ng-star-inserted'])[" + str(idx) + "]")))
                    click_address.click()

                    logger.removeHandler(f_handler)
                    logger.removeHandler(c_handler)
                    logging.shutdown()

                    return True

                elif true_address != mls_address:
                    continue

                else:

                    logger.removeHandler(f_handler)
                    logger.removeHandler(c_handler)
                    logging.shutdown()

                    return False

    @staticmethod
    def rpr_sq_ft(driver_var):

        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, 'html.parser')

        key_facts = soup.find('section', {"class": "key-facts"})

        sq_ft = key_facts.div.div.span.get_text()

        return sq_ft

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
    def search_listing(mls_number, driver_var, logger_var, mls_address=None):
        # Type in the MLS number or address. Create own function thats able to use both if necessary
        search_listing = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, 'qcksrchmlstxt')))
        search_listing.click()
        search_listing.send_keys(mls_number)

        time.sleep(3.5)  # Built in latency to allow the table to populate
        page_results = driver_var.page_source

        return page_results

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
    def sq_ft_keyerror(mls_address, mls_db, tax_db, outer_address_list, **kwargs):

        logger = kwargs['logger']

        change_dict = {'Rd': 'Road', 'Ct': 'Court', 'St': 'Street', 'Ave': 'Avenue',
                       'Dr': 'Drive', 'Ln': 'Lane', 'Pl': 'Place', 'Ter': 'Terrace', 'Hwy': 'Highway',
                       'Pkwy': 'Parkway', 'Cir': 'Circle'}
        clean_pattern = re.compile(r'(Rd$|Ct$|St$|Ave$|Dr$|Ln$|Pl$|Ter$|Hwy$|Pkwy$|Cir$)', flags=re.IGNORECASE)
        space_pattern = re.compile(r'(\xa0)')
        address_pattern = re.compile(r'(.*)')
        mls_number = str(mls_db.loc[mls_address, 'MLSNUM'])
        address_list = GSMLS.property_archive(mls_number, mls_address, **kwargs)

        if address_list == 'No historical addresses available':
            logger.info(f'There are no historical addresses available for {mls_address}')
            for idx, addy1 in enumerate(outer_address_list):
                if mls_address[:10].upper() in addy1:
                    final_address = outer_address_list[idx]
                    logger.info(f'Partial match for {addy1} found in outer list. Sqft can now be found')

                    return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list,
                                              transformed_address=final_address, **kwargs)

                elif (mls_address[:10].upper() not in addy1) and (addy1 != outer_address_list[-1]):
                    continue

                elif (mls_address[:10].upper() not in addy1) and (addy1 == outer_address_list[-1]):

                    if int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                        pass

                    else:
                        # No address works
                        logger.info(
                            f'None of the addresses meets the database search criteria. Now initiating the RPR function.')
                        full_address = mls_address + ', ' + mls_db.loc[mls_address, 'TOWN']
                        rpr_results = GSMLS.rpr('SQFT', full_address)
                        if rpr_results > 0:
                            logger.info(f'RPR sqft results for {mls_address} have been found: {rpr_results}')
                            mls_db.at[mls_address, 'SQFTAPPROX'] = rpr_results

                        elif int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                            pass

                        else:
                            logger.info(f'RPR sqft results for {mls_address} have not been found')
                            mls_db.at[mls_address, 'SQFTAPPROX'] = 0
        else:
            logger.info(f'A list of addresses similar to {mls_address} have been found. Looping through the list until an address which matches the criteria is found')
            for addy in address_list:
                try:
                    space_search = space_pattern.search(addy).group()
                    space_found = addy.replace(space_search, ' ')
                    change_search = clean_pattern.search(space_found).group()
                    # change_found = space_found.replace(change_search, change_dict[change_search.title()])
                    change_found = change_dict[change_search.title()].join(space_found.rsplit(change_search, maxsplit=1))
                    address_search = address_pattern.search(change_found)
                    final_address = GSMLS.clean_addresses(address_search)

                except AttributeError:
                    if addy != list(address_list)[-1]:
                        logger.info(f'{addy} does not meet the database search criteria. Continuing through the loop')
                        continue

                    elif addy.upper() not in outer_address_list:
                        space_search = space_pattern.search(addy).group()
                        space_found = addy.replace(space_search, ' ')
                        for idx, addy1 in enumerate(outer_address_list):
                            if space_found[:10].upper() in addy1:
                                final_address = outer_address_list[idx]
                                logger.info(f'Partial match for {addy} found in outer list. Sqft can now be found')

                                return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list,
                                                          transformed_address=final_address, **kwargs)

                            elif (space_found[:10].upper() not in addy1) and (addy1 != outer_address_list[-1]):
                                continue

                            elif (space_found[:10].upper() not in addy1) and (addy1 == outer_address_list[-1]):

                                if int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                                    pass

                                else:
                                    # No address works
                                    logger.info(f'None of the addresses meets the database search criteria. Now initiating the RPR function.')
                                    full_address = addy.title() + ', ' + mls_db.loc[mls_address, 'TOWN']
                                    rpr_results = GSMLS.rpr('SQFT', full_address)
                                    if rpr_results > 0:
                                        logger.info(f'RPR sqft results for {mls_address} have been found: {rpr_results}')
                                        mls_db.at[mls_address, 'SQFTAPPROX'] = rpr_results

                                    elif int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                                        pass

                                    else:
                                        logger.info(f'RPR sqft results for {mls_address} have not been found')
                                        mls_db.at[mls_address, 'SQFTAPPROX'] = 0

                else:

                    if final_address.upper() in outer_address_list:
                        logger.info(f'{final_address} meets the database search criteria')

                        return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list,
                                                  transformed_address=final_address, **kwargs)
                    elif final_address.upper() not in outer_address_list:

                        for idx, addy1 in enumerate(outer_address_list):
                            if final_address[:10].upper() in addy1:
                                final_address = outer_address_list[idx]
                                logger.info(f'Partial match for {final_address} found in outer list. Sqft can now be found')

                                return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list,
                                                          transformed_address=final_address, **kwargs)

                            elif (space_found[:10].upper() not in addy1) and (addy1 != outer_address_list[-1]):
                                continue

                            elif (space_found[:10].upper() not in addy1) and (addy1 == outer_address_list[-1]):

                                if addy != list(address_list)[-1]:
                                    continue

                                elif int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                                    pass

                                else:
                                    # No address works
                                    logger.info(
                                        f'None of the addresses meets the database search criteria. Now initiating the RPR function.')
                                    full_address = addy.title() + ', ' + mls_db.loc[mls_address, 'TOWN']
                                    rpr_results = GSMLS.rpr('SQFT', full_address)
                                    if rpr_results > 0:
                                        logger.info(f'RPR sqft results for {mls_address} have been found: {rpr_results}')
                                        mls_db.at[mls_address, 'SQFTAPPROX'] = rpr_results

                                    elif int(mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                                        pass

                                    else:
                                        logger.info(f'RPR sqft results for {mls_address} have not been found')
                                        mls_db.at[mls_address, 'SQFTAPPROX'] = 0

        return mls_db

    @staticmethod
    def sq_ft_search(mls_address, mls_db, tax_db, outer_address_list, transformed_address=None, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        if transformed_address is None:
            # Sq ft search for non numbered blocks
            try:
                if mls_db.loc[mls_address, 'SQFTAPPROX'] == 0:
                    mls_db.at[mls_address, 'SQFTAPPROX'] = tax_db.loc[mls_address.upper(), 'Sq. Ft.']
                elif mls_db.loc[mls_address, 'SQFTAPPROX'] != 0:
                    if mls_db.loc[mls_address, 'SQFTAPPROX'] == tax_db.loc[mls_address.upper(), 'Sq. Ft.']:
                        pass
                    elif (tax_db.loc[mls_address.upper(), 'Sq. Ft.'] == 0) and int(
                            mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                        pass
                    else:
                        mls_db.at[mls_address, 'SQFTAPPROX'] = tax_db.loc[mls_address.upper(), 'Sq. Ft.']

            except ValueError:
                logger.info(f'Multiple addresses @ {mls_address} have been found. The search will be narrowed to find the correct one')
                year_built = mls_db.loc[mls_address, 'YEARBUILT']
                tax_db = tax_db[(tax_db['Yr. Built'] == year_built) & (tax_db['Property Location'] == mls_address.upper())]

                # GSMLS.kill_logger(logger, f_handler, c_handler)

                return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list, **kwargs)

            except KeyError:
                logger.warning(f'{mls_address} was not found in the tax database. Sq_ft_keyerror will be initiated to find the correct address')
                # GSMLS.kill_logger(logger, f_handler, c_handler)

                return GSMLS.sq_ft_keyerror(mls_address, mls_db, tax_db, outer_address_list, **kwargs)

        else:
            # Sq ft search for numbered blocks
            try:
                if mls_db.loc[mls_address, 'SQFTAPPROX'] == 0:
                    mls_db.at[mls_address, 'SQFTAPPROX'] = tax_db.loc[transformed_address.upper(), 'Sq. Ft.']
                elif mls_db.loc[mls_address, 'SQFTAPPROX'] != 0:
                    if mls_db.loc[mls_address, 'SQFTAPPROX'] == tax_db.loc[transformed_address.upper(), 'Sq. Ft.']:
                        pass
                    elif (tax_db.loc[transformed_address.upper(), 'Sq. Ft.'] == 0) and int(
                            mls_db.loc[mls_address, 'SQFTAPPROX']) > 0:
                        pass
                    else:
                        mls_db.at[mls_address, 'SQFTAPPROX'] = tax_db.loc[transformed_address.upper(), 'Sq. Ft.']

            except ValueError:
                logger.info(
                    f'Multiple addresses @ {mls_address} have been found. The search will be narrowed to find the correct one')
                year_built = mls_db.loc[mls_address, 'YEARBUILT']
                tax_db = tax_db[(tax_db['Yr. Built'] == year_built) & (tax_db['Property Location'] == transformed_address.upper())]

                # GSMLS.kill_logger(logger, f_handler, c_handler)

                return GSMLS.sq_ft_search(mls_address, mls_db, tax_db, outer_address_list, transformed_address, **kwargs)

            except KeyError:
                logger.warning(
                    f'{mls_address} was not found in the tax database. Sq_ft_keyerror will be initiated to find the correct address')
                # GSMLS.kill_logger(logger, f_handler, c_handler)

                return GSMLS.sq_ft_keyerror(mls_address, mls_db, tax_db, outer_address_list, **kwargs)

        # GSMLS.kill_logger(logger, f_handler, c_handler)

        return mls_db

    @staticmethod
    def sql_table_check(cursor_var, table_name):

        # https://stackoverflow.com/questions/1874113/checking-if-a-postgresql-table-exists-under-python-and-probably-psycopg2
        # https://thepythoncode.com/article/use-gmail-api-in-python
        # https://stackoverflow.com/questions/51054245/read-mails-from-custom-label-in-gmail-using-pythongoogle-api
        # https://stackoverflow.com/questions/41800867/gmail-api-using-multiple-labelids

        cursor_var.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)", (table_name, ))

        return cursor_var.fetchone()[0]

    @staticmethod
    def target_counties_res(series):
        """
        Calculates the average attributable percentage of state sales for a county then creates a dictionary
        of counties who's attributaable sales are over that average for the quarter

        :param series:
        :return:
        """
        total_sum_county = series.sort_values().sum()
        county_list = series.index.to_list()
        county_sales_per_list = []
        for cumsum in series.to_list():
            county_sales_per_list.append(cumsum / total_sum_county)

        county_sales_avg = round((mean(county_sales_per_list)) * 100, 3)

        county_farm_dict = {}
        for county, county_sum in zip(county_list, series.to_list()):
            if ((county_sum / total_sum_county) * 100) > county_sales_avg:
                county_farm_dict.setdefault(county, [])
                county_farm_dict[county].append(county_sum)
                county_farm_dict[county].append(round((county_sum / total_sum_county) * 100, 3))

        return county_farm_dict

    @staticmethod
    def total_units(db):

        temp_db = db
        temp_db = temp_db.astype({'UNIT1BATHS': 'string', 'UNIT2BATHS': 'string',
                             'UNIT3BATHS': 'string', 'UNIT4BATHS': 'string'})

        temp_dict = {
            'unit1': temp_db['UNIT1BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace('np.nan', '0'),
            'unit2': temp_db['UNIT2BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace('np.nan', '0'),
            'unit3': temp_db['UNIT3BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace('np.nan', '0'),
            'unit4': temp_db['UNIT4BATHS'].str.replace(r'\d{1}|\d{1}.\d{1,30}?', '1', regex=True)
            .str.replace('np.nan', '0')
        }

        temp_db2 = pd.DataFrame(temp_dict).fillna(value='0')
        temp_db2 = temp_db2.astype({'unit1': 'int64', 'unit2': 'int64', 'unit3': 'int64', 'unit4': 'int64'})

        db['TOTALUNITS'] = temp_db2['unit1'] + temp_db2['unit2'] + temp_db2['unit3'] + temp_db2['unit4']

        db.insert(6, 'TOTALUNITS', db.pop('TOTALUNITS'))

        print(db.columns)

        return db

    @staticmethod
    def total_units_statistics(db, **kwargs):
        # Can only be used with the MUL property type

        property_type = str(db.__name__)[-3:].upper()

        if property_type == 'MUL':
            unit_columns = ['COUNTY', 'TOWN', 'TOTALUNITS']
            units_df = db[unit_columns].groupby(['COUNTY', 'TOWN'])
            units_stats = units_df.value_counts()

            return units_stats
        else:
            raise ValueError

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
            GSMLS.login('GSMLS', driver)
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
        # obj.property_archive('3819260')
