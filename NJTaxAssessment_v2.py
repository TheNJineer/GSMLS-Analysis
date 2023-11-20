import os
import re
import shutil
import time
from zipfile import ZipFile
import pandas as pd
import send2trash
import threading
from datetime import datetime
from traceback import format_tb
from bs4 import BeautifulSoup
import logging
import requests
import selenium
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import ElementNotSelectableException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import NoSuchAttributeException
from selenium.common.exceptions import NoSuchDriverException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
# Allows for Selenium to click a button
from selenium.webdriver.support.select import Select


class NJTaxAssessment:

    tax_assessment = 'https://tax1.co.monmouth.nj.us/cgi-bin/prc6.cgi?menu=index&ms_user=monm&district=1301&mode=11'
    coordinates = 'https://www.latlong.net/'
    state_county_dict = None

    def __init__(self, city=None, county=None):
        # What information do I need to initialize an instance of this class?
        try:

            if (city and county) is not None:
                self._city = city.upper()
                self._county = county.upper()
                self._database = NJTaxAssessment.city_database(self._county,self._city)
            elif (city and county) is None:
                self._city = None
                self._county = None
                self._database = None
            else:
                raise AssertionError

        except AssertionError:
            print('Error!\nCity and County have to equal values or equal None')

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

    def auction_locations(self, city):
        # Use the GSMLS Tax statesinfo website to scrap all the NJ Counties Foreclosure Auction Locations
        pass

    @property
    def city(self):
        return self._city

    @city.setter
    def city(self, county, value):
        self._city = value.upper()
        self._database = NJTaxAssessment.city_database(county, value)

    @property
    def county(self):
        return self._county

    @county.setter
    def county(self, value):
        self._county = value.upper()
        self._database = NJTaxAssessment.city_database(self.county, value)

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self,county, value):
        self._database = NJTaxAssessment.city_database(county, value)

    # @staticmethod
    # def property_tax_legend():
    #     # Produce the NJ Property tax system legend database to be used
    #     # to give greater detail on what type of property is at the target address
    #
    #     filename = ''
    #     db = pd.read_csv(filename)
    #
    #     return db

    @staticmethod
    @logger_decorator
    def all_county_scrape(key, counties, driver_var, county_name=None, city_name=None, **kwargs):
        """

        :param key:
        :param counties:
        :param driver_var:
        :param county_name:
        :param city_name:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        # started_threads = []

        nj_tax_base_url = 'https://tax1.co.monmouth.nj.us/'

        try:
            county_option = WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='normdiv']/form/table[1]/tbody/tr[3]/td[2]/select/option["
                     + str(key) + "]")))  # Make sure the XPATH includes the variable j
            logger.info(f'Now downloading the databases for {counties[key]} County')
            county_option.click()  # Step 1: Select the county
            results1 = driver_var.page_source
            cities = NJTaxAssessment.find_cities(results1)

            if (county_name and city_name) is None:
                with requests.Session() as session:
                    for k in cities.keys():
                        try:
                            city_option = driver_var.find_element(By.XPATH,
                                                                  "//*[@id='normdiv']/form/table[1]/tbody/tr[4]/td[2]/select/option["
                                                                  + str(k) + "]")  # Make sure the XPATH includes the variable k
                            results = NJTaxAssessment.database_check(counties[key], cities[k], logger)
                            if results == 'File Already Exists':
                                continue
                            else:
                                city_option.click()
                                output_option = driver_var.find_element(By.XPATH,
                                                                        "//*[@id='normdiv']/form/table[1]/tbody/tr[6]/td[2]/select")
                                output_option.click()  # Step 2: Select the city
                                excel_option = driver_var.find_element(By.XPATH,
                                                                       "//*[@id='normdiv']/form/table[1]/tbody/tr[6]/td[2]/select/option[3]")
                                excel_option.click()  # Step 3: Select Excel/CSV as the file output
                                submit_search = driver_var.find_element(By.XPATH,
                                                                        "//*[@id='normdiv']/form/table[3]/tbody/tr[2]/td[2]/input[1]")
                                submit_search.click()  # Step 4: Click the submit button to generate the zip file
                                time.sleep(1) # Give the page time to fully load. Have had instances where download link isn't captured
                                results2 = driver_var.page_source
                                download_link = NJTaxAssessment.find_zipfile(results2)
                                NJTaxAssessment.stream_zipfile(nj_tax_base_url, download_link, session)
                                NJTaxAssessment.unzip_and_extract(counties[key], cities[k], download_link.split('/')[2])
                                logger.info(f'The download for {cities[k]} has finished...')
                                driver_var.back()  # Step &: Go back to the previous page to start the loop over again
                                # Find out how to go back. The new search button doesn't work

                        except WebDriverException as WDE:
                            logger.exception(f'{WDE}')

            elif county_name is not None and city_name is None:
                pass

            elif (county_name and city_name) is not None:
                pass

        except ElementNotVisibleException as ENV:  # Make more specific exception handling blocks later
            logger.exception(f'{ENV}')

        except ElementNotSelectableException as ENS:
            logger.exception(f'{ENS}')

        except InvalidArgumentException as IAE:
            logger.exception(f'{IAE}')

        except NoSuchAttributeException as NSAE:
            logger.exception(f'{NSAE}')

        except NoSuchDriverException as NSDE:
            logger.exception(f'{NSDE}')

        except NoSuchElementException as NSEE:
            logger.exception(f'{NSEE}')

        except WebDriverException as WDE:
            logger.exception(f'{WDE}')

        else:

            # for threadobj in started_threads:
            #     threadobj.join()

            logger.info(f'All databases for {counties[key]} County have been downloaded')
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

    @staticmethod
    def city_database(county, city):
        """

        :param county:
        :param city:
        :return:
        """
        # produce the property database for the specified city

        base_path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Property Data'
        target_path = os.path.join(base_path, county.upper())
        city_list = os.listdir(target_path)
        county = county.upper()
        try:
            if county == 'ESSEX':

                for i in city_list:
                    if city not in i:
                        continue
                    elif city in i:
                        filename = os.path.join(target_path, i + '.xlsx')
                        db = pd.read_excel(filename, header=0)
                    else:
                        raise IndexError(f'{city} does not exist in {county} County')
            else:
                for i in city_list:
                    if city not in i:
                        continue
                    elif city in i:
                        filename = os.path.join(target_path, i + '.csv')
                        db = pd.read_csv(filename, header=0)
                    else:
                        raise IndexError(f'{city} does not exist in {county} County')
        except IndexError as IE:
            print(f'{IE}')
        else:
            return db

    @staticmethod
    def database_check(county, city, logger):

        base_path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Property Data'
        target_path = os.path.join(base_path, county, city)

        if os.path.exists(target_path):
            latest_file = os.listdir(target_path)[-1]
            if str(datetime.today().year) in str(latest_file):
                logger.info(f'Database for {city} already exists')

                return 'File Already Exists'

        else:
            return "File Doesn't Exist"

    @staticmethod
    def essex_county_scrape(driver_var, city_name=None, **kwargs):
        """

        :param driver_var:
        :param city_name:
        :param kwargs:
        :return:
        """
        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        started_threads = []

        url = 'https://www.taxdatahub.com/6229fbf0ce4aef911f9de7bc/Essex%20County'

        driver_var.get(url)

        try:
            driver_var.fullscreen_window()
            WebDriverWait(driver_var, 10).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='side-bar']/div[1]/h3")))

            results = driver_var.page_source
            cities = NJTaxAssessment.find_essex_cities(results)

            if city_name is None:
                town_filter = WebDriverWait(driver_var, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='town-filter-el']/a/label")))
                town_filter.click()
                for k in cities.keys():
                    try:
                        city_option = WebDriverWait(driver_var, 10).until(EC.presence_of_element_located(
                                                (By.XPATH, "//*[@id=" + k + "]")))  # Make sure the XPATH includes the variable k
                        results = NJTaxAssessment.database_check('ESSEX', cities[k], logger)
                        if results == 'File Already Exists':
                            continue
                        else:
                            city_option.click()
                            time.sleep(2)
                            download_link = WebDriverWait(driver_var, 10).until(EC.presence_of_element_located(
                                                (By.XPATH, "//a[normalize-space()='Download Excel']")))
                            download_link.click()
                            time.sleep(2)
                            accept_disclaimer = WebDriverWait(driver_var, 10).until(EC.presence_of_element_located(
                                                (By.XPATH,  "//*[@id='notice-modal-download']/div/div/div[3]/button")))
                            accept_disclaimer.click()
                            # time.sleep(1.5)
                            waiting_for_download = WebDriverWait(driver_var, 30).until(EC.visibility_of_element_located(
                                                (By.XPATH, "//*[@id='search-bar']/div[2]/div[3]/div/button/span")))
                            while waiting_for_download:
                                download_done = WebDriverWait(driver_var, 30).until(EC.visibility_of_element_located(
                                                (By.XPATH, "//a[normalize-space()='Download Excel']")))
                                if not download_done:
                                    continue
                                else:
                                    # NJTaxAssessment.unzip_and_extract('ESSEX', cities[k])
                                    logger.info(f'The download for {cities[k]} has finished...')
                                    time.sleep(1)
                                    clear_filter = WebDriverWait(driver_var, 10).until(EC.presence_of_element_located(
                                        (By.XPATH, "//*[@id='town-filter-div']/label[1]")))
                                    clear_filter.click()
                                    break
                    except ElementNotInteractableException as ENI:
                        logger.exception(f'{ENI}')
                    except WebDriverException as WDE:
                        logger.exception(f'{WDE}')

            elif city_name is not None:
                pass

        except ElementNotVisibleException as ENV:  # Make more specific exception handling blocks later
            logger.exception(f'{ENV}')

        except ElementNotInteractableException as ENI:
            logger.exception(f'{ENI}')

        except ElementNotSelectableException as ENS:
            logger.exception(f'{ENS}')

        except InvalidArgumentException as IAE:
            logger.exception(f'{IAE}')

        except NoSuchAttributeException as NSAE:
            logger.exception(f'{NSAE}')

        except NoSuchDriverException as NSDE:
            logger.exception(f'{NSDE}')

        except NoSuchElementException as NSEE:
            logger.exception(f'{NSEE}')

        except WebDriverException as WDE:
            logger.exception(f'{WDE}')

        else:

            for threadobj in started_threads:
                threadobj.join()

            logger.info(f'All databases for ESSEX County have been downloaded')
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

    # @classmethod
    # def find_county(cls, city):
    #
    #     return cls.state_county_dict.loc[city, 'County']

    # @staticmethod
    # def find_city(county):
    #
    #     return NJTaxAssessment.state_county_dict.loc[city, 'County']

    @staticmethod
    def find_counties(page_source):
        """

        :param page_source:
        :return:
        """
        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'\d{4}')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find('select', {"name": "select_cc"})
        target_contents = target.find_all('option', {'value': value_pattern})
        counties = {}

        for idx, i in enumerate(target_contents):
            counties[idx + 1] = i.get_text()

        keys = list(counties.keys())

        counties[keys[-1] + 1] = "ESSEX"  # Manually add ESSEX county to the dictionary because it's not in page source

        NJTaxAssessment.state_county_dictionary(counties)

        return counties

    @staticmethod
    def find_cities(page_source):
        """

        :param page_source:
        :return:
        """
        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'\d{4}')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find('select', {"name": "district"})
        target_contents = target.find_all('option', {'value': value_pattern})
        cities = {}

        for idx, i in enumerate(target_contents):
            if i.get_text() == 'ALL':
                break
            else:
                cities[idx + 1] = i.get_text()

        return cities

    @staticmethod
    def find_essex_cities(page_source):
        """

        :param page_source:
        :return:
        """
        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'\d{4}')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find('div', {"id": "town-filter-div"})
        target_contents = target.find_all('input', {'id': value_pattern})
        cities = {}

        for i in target_contents:
            cities[i['id']] = i['value']

        return cities

    @staticmethod
    def find_zipfile(page_source):
        """

        :param page_source:
        :return:
        """
        try:
            # Find the zipfile for the NJ Tax Assessment download
            download_pattern = re.compile(r'(../download/(.*.zip))')
            soup = BeautifulSoup(page_source, 'html.parser')
            target = soup.find('a', {'href': download_pattern.search(str(soup)).group(0)})
            temp_name = download_pattern.search(str(target)).group(1)

        except Exception as e:
            print(f'{e}')

        return temp_name

    # @staticmethod
    # def foreclosure_listings(county):
    #     # Uses https://salesweb.civilview.com/ and https://www.foreclosurelistings.com/list/NJ/
    #     # to pull the latest pending foreclosures for a county and potential property buys
    #     pass

    @logger_decorator
    def long_lat(self, driver_var, county, city, **kwargs):
        """

        :param driver_var:
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

        city_db = NJTaxAssessment.city_database(county, city)
        good_property_pattern = re.compile(r'(\d{1,5}?-\d{1,5}?|\d{1,5}?)\s(.*)')
        bad_property_pattern = re.compile(r'(\w+\s){0,10}?')
        url = NJTaxAssessment.coordinates
        filename = city + ' Database ' + datetime.now().year
        driver_var.get(url)

        try:
            property_address_list = city_db['Property Location'].to_list()
            city_db = city_db.set_index('Property Location')
            for i in property_address_list:
                if bad_property_pattern.search(i) is not None:
                    continue
                elif good_property_pattern.search(i) is not None:
                    address = ', '.join([good_property_pattern.search(i).group(), city, 'NJ'])
                    place_name = WebDriverWait(driver_var, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//*[@id='place']")))
                    place_name.click()
                    place_name.send_keys(address)
                    search = driver_var.find_element(By.XPATH, "//*[@id='btnfind']")
                    search.click()
                    value = WebDriverWait(driver_var, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//*[@id='latlngspan']")))
                    lat_long = value.get_text().lstrip('(').rstrip(')').split(',')
                    latitude = float(lat_long[0].strip(''))
                    longitude = float(lat_long[1].strip(''))
                    if city_db.loc[i, 'Latitude'] == 0 or city_db.loc[i, 'Latitude'] == '0':
                        city_db.at[i, 'Latitude'] = latitude
                    if city_db.loc[i, 'Longitude'] == 0 or city_db.loc[i, 'Longitude'] == '0':
                        city_db.at[i, 'Longitude'] = longitude

        except ElementNotVisibleException as ENV:  # Make more specific exception handling blocks later
            logger.exception(f'{ENV}')

        except ElementNotSelectableException as ENS:
            logger.exception(f'{ENS}')

        except InvalidArgumentException as IAE:
            logger.exception(f'{IAE}')

        except NoSuchAttributeException as NSAE:
            logger.exception(f'{NSAE}')

        except NoSuchDriverException as NSDE:
            logger.exception(f'{NSDE}')

        except NoSuchElementException as NSEE:
            logger.exception(f'{NSEE}')

        except WebDriverException as WDE:
            logger.exception(f'{WDE}')

        finally:
            with pd.ExcelWriter(filename) as writer:
                # Switch to new directory
                city_db.to_excel(writer, sheet_name=city + ' Properties')
                # Switch to old directory

            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

    @staticmethod
    @logger_decorator
    def nj_databases(driver_var, county_name=None, city_name=None, **kwargs):
        """

        :param driver_var:
        :param county_name:
        :param city_name:
        :param kwargs:
        :return:
        """
        # Method which can download all the ownership information for
        # existing homes in NJ off of the NJ Tax Assessment website

        try:
            # assert (city and county is None) or (city and county is not None)
            url = NJTaxAssessment.tax_assessment
            driver_var.get(url)

            results = driver_var.page_source
            counties = NJTaxAssessment.find_counties(results)

            if (county_name and city_name) is None:
                for j in counties.keys():
                    if counties[j] == 'ESSEX':
                        NJTaxAssessment.essex_county_scrape(driver_var, **kwargs)
                    else:
                        NJTaxAssessment.all_county_scrape(j, counties, driver_var, **kwargs)
            elif county_name is not None and city_name is None:
                pass

            elif county_name and city_name is not None:
                pass

        except Exception as E:
            print(f'{E}')

    @classmethod
    def state_county_dictionary(cls, counties):
        """

        :param counties:
        :return:
        """
        cls.state_county_dict = counties

    @staticmethod
    def stream_zipfile(base_url, download_param, session_var):

        previous_dir = os.getcwd()
        os.chdir('C:\\Users\\Omar\\Desktop\\Selenium Temp Folder')
        new_filename = download_param.split('/')[2]
        full_url = "".join([base_url, download_param.lstrip('../')])

        with session_var.get(full_url, stream= True) as reader, open(new_filename, 'wb') as writer:
            for chunk in reader.iter_content(chunk_size=1000000):
                writer.write(chunk)

        os.chdir(previous_dir)

    def tax_lien_foreclosure(self, county=None, city=None):
        """

        :param county:
        :param city:
        :return:
        """
        # Creates a database of all the city owned and vacant lots
        # and properties for future pursuit. Uses the nj_database file

        if city and county is None:
            db = self.database

        else:
            db = NJTaxAssessment.city_database(county, city)

        if 'EPL_Code' in db.columns:  # Only used for municipalities in Essex County
            tax_lien_db = db[db['EPL_Code'] == '0401047']
        else:
            tax_lien_db = db[(db['EPL Own'] == 4) & (db['EPL Use'] == 1) & (db['EPL Desc'] == 47)]

        return tax_lien_db

    @staticmethod
    def unzip_and_extract(county, city, temp_file_name = None):
        """
        Will find the recently downloaded zip file of the city
        for which all the property information is located. This function will accept the
        temporary file name and city as arguments and rename the file with the respective
        city in the name and store it in the specific directory under that county
        :param county:
        :param city:
        :param temp_file_name:
        :return:
        """

        previous_dir = os.getcwd()
        path = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'
        base_path = 'F:\\Real Estate Investing\\JQH Holding Company LLC\\Property Data'
        os.chdir(path)
        filenames = os.listdir(path)

        try:
            for file in filenames:
                target_path = os.path.join(base_path, county, city)
                if temp_file_name is not None:
                    if temp_file_name != file:
                        continue

                    elif temp_file_name == file:
                        extract_file = ZipFile(os.path.abspath(file))
                        target_file = temp_file_name.rstrip('.zip') + '.csv'
                        extract_file.extract(target_file)
                        extract_file.close()

                        if os.path.exists(target_path):
                            time.sleep(0.5)
                            shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
                                                        + 'Database' + ' ' + str(datetime.today().date()) + '.csv'))
                            send2trash.send2trash(file)
                        else:
                            os.makedirs(target_path)
                            time.sleep(0.5)
                            shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
                                                            + 'Database' + ' ' + str(datetime.today().date()) + '.csv'))
                            send2trash.send2trash(file)
                elif file.startswith('TaxData'):
                    target_file = file + '.xlsx'
                    if os.path.exists(target_path):
                        time.sleep(0.5)
                        shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
                                                        + 'Database' + ' ' + str(datetime.today().date()) + '.xlsx'))
                    else:
                        os.makedirs(target_path)
                        time.sleep(0.5)
                        shutil.move(os.path.abspath(target_file), os.path.join(target_path, city + ' '
                                                            + 'Database' + ' ' + str(datetime.today().date()) + '.xlsx'))
                # else:
                #     if temp_file_name is not None:
                #         raise IndexError(f"File Does Not Exist: {temp_file_name}")
                #     else:
                #         raise IndexError(f"File Does Not Exist", county, city)
        except IndexError as IE:
            print(f'{IE} ----> {city}')

        except Exception as E:
            print(f'{E}')

        else:
            os.chdir(previous_dir)

    def vacant_land(self, county=None, city=None):
        """

        :param county:
        :param city:
        :return:
        """
        # Creates a database of all the city owned and vacant lots
        # and properties for future pursuit. Uses the nj_database file

        if city and county is None:
            db = self.database

        else:
            db = NJTaxAssessment.city_database(county, city)

        if 'EPL_Code' in db.columns:  # Only used for municipalities in Essex County
            vacant_land_db = db[db['PropertyClassCode'] == 1]
        else:
            vacant_land_db = db[db['Property Class'] == 1]

        return vacant_land_db

    @staticmethod
    def waiting(sleep_time):
        """

        :param sleep_time:
        :return:
        """
        sleep_time2 = str(sleep_time.days)
        sleep_time3 = int(sleep_time2) * 86400  # 86,400 seconds in a day
        if sleep_time3 > 86400:
            # message_body = f'There is currently no new data available. NJRScrapper will check again in {sleep_time.days} days...'
            # Scraper.text_message(message_body)
            time.sleep(sleep_time3)
        else:
            # message_body = f"There is currently no new data available. Will check again tomorrow..."
            # Scraper.text_message(message_body)
            time.sleep(86400)

    def main(self):
        try:
            save_location1 = 'C:\\Users\\jibreel.q.hameed\\Desktop\\Selenium Temp Folder'
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

            NJTaxAssessment.nj_databases(driver)

        except Exception as e:
            print(e)


if __name__ == '__main__':

    obj = NJTaxAssessment()
    obj.main()
