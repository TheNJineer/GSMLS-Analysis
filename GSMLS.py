import json
import re
import send2trash
import time
import os
import calendar
import bs4.element
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import sys, traceback
from tqdm import tqdm
from tqdm.auto import trange
from sqlalchemy import create_engine
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains as AC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import UnexpectedAlertPresentException, WebDriverException
from kafka import KafkaProducer
from pprint import pprint


# Custom class created to handle the console logging while using the tqdm progress bar
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)

class GSMLS:

    def __init__(self):
        self.counties = {}
        self.municipalities = {}
        self.rows_counted = 0
        self.download_log = {
            'Year_': [],
            'Quarter': [],
            'County': [],
            'Municipality': [],
            'Initiated': [],
            'Results_Found': [],
            'Finished': [],
            'Rows_Produced': [],
            'Date_Produced': [],
            'Property_Type': [],
        }
        self.engine = GSMLS.create_engine()
        self.last_scraped_qtr = None
        self.last_scraped_year = None
        self.last_scraped_county = None
        self.last_scraped_muni = None
        self.last_scraped_property_type = None
        self.finished = None
        self.timeframe = 'historic'
        self.load_metadata()

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
                filepath = 'F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Logs'
                log_filepath = os.path.join(filepath, original_function.__name__ + ' ' + str(datetime.today().date()) + '.log')
                f_handler = logging.FileHandler(log_filepath)
                f_handler.setLevel(logging.DEBUG)
                c_handler = TqdmLoggingHandler()
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

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    @staticmethod
    def clean_address(address):

        target = address.split(',')
        raw_address = ' '.join(target[0].rstrip('*').strip().split(' '))
        city = target[1].rstrip('*').strip()
        raw_address = ' '.join([i.strip() for i in raw_address.split('\xa0')])
        clean_address = ', '.join([raw_address, city])

        return clean_address

    @staticmethod
    def click_target_tab(target_name, driver_var):

        target_dict = {
            'County': 1,
            'Status': 2,
            'Town': 3,
            'Property_Type': 8
        }

        # Locate and click the County tab
        x_path = f'//*[@id="advance-search-fields"]/li[{target_dict[target_name]}]'
        property_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path)))
        property_tab.click()
        time.sleep(1)

    @staticmethod
    def click_property_type(type_, driver_var):

        # Locate and click the property type in the Advanced Search
        x_path = f"//select[@id='ptype']"
        property_menu = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path)))
        property_menu.click()
        choice_path = f"//option[@value='{type_}']"
        WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, choice_path))).click()
        time.sleep(1)

    @staticmethod
    def clean_addresses(search_string):
        """Used inside of the find sq_ft function"""

        target_list = str(search_string.group()).split(' ')
        new_address_list = [i for i in target_list if i != '']

        return ' '.join(new_address_list)

    @staticmethod
    def create_engine():

        username, base_url, pw = GSMLS.get_us_pw('PostgreSQL')
        engine = create_engine(f"postgresql+psycopg2://{username}:{pw}@{base_url}:5432/gsmls")

        return engine

    def create_state_dictionary(self, driver_var):

        results = driver_var.page_source
        self.find_counties(results)

        print('Preparing State Dictionary...')
        for _, county_id in zip(trange(len(self.counties.keys()), desc='Counties'), self.counties.keys()):
            self.scrape_municipalities(county_id, driver_var)

        print('State Dictionary completed. Data will be scraped shortly...')

    @staticmethod
    def download_complete(filename):

        download_folder = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'
        for _ in range(1,11):
            abspath_ = os.path.join(download_folder, filename + '.xls')
            if os.path.exists(abspath_):
                return True
            else:
                time.sleep(0.5)

        return False
        # raise TimeoutError(f'{filename} did not download in a timely manner')

    @staticmethod
    def download_sales_data(city_name, county_name, qtr, year, prop_type, driver_var, window_id):

        GSMLS.explicit_page_load('Results', driver_var, property_type=prop_type)

        page_source = driver_var.page_source

        # Check all items to be downloaded
        check_all_results = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.ID, 'checkall')))
        check_all_results.click()

        # Locate the download button and click
        download_idx = GSMLS.find_link_index('Download', page_source)
        download_results = driver_var.find_element(By.XPATH, f'//*[@id="sub-navigation-container"]/div/nav[1]/a[{download_idx}]')
        download_results.click()

        GSMLS.explicit_page_load('Download', driver_var)
        download_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Download']")))

        # Locate the option to download as a xls file and name it
        excel_file_input = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, 'downloadfiletype3')))
        excel_file_input.click()
        filename_input = driver_var.find_element(By.ID, 'filename')
        filename_input.click()
        filename = city_name.rstrip('.') + ' ' + county_name + ' ' + 'Q'+str(qtr) + str(year) + f' {prop_type} Sales GSMLS'
        AC(driver_var).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(filename).perform()
        time.sleep(0.5)

        # Request the download and close the page
        download_button.click()
        driver.switch_to.window(window_id)
        close_page = driver_var.find_element(By.XPATH, "//*[@id='sub-navigation-container']/div/nav[1]/a[2]")
        close_page.click()

        return filename

    @staticmethod
    def exit_results_page(driver_var):

        page_source = driver_var.page_source
        close_idx = GSMLS.find_link_index('Close', page_source)
        close_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, f'//*[@id="sub-navigation-container"]/div/nav[1]/a[{close_idx}]')))
        close_button.click()
        time.sleep(1.5)  # Built-in latency

    @staticmethod
    def explicit_page_load(page_name, driver_var, property_type=None, prop_id=None, window_id=None):

        prop_dict = {'RES': 'Residential',
                     'MUL': 'Multi-Family',
                     'LND': 'Land',
                     'RNT': 'Rental',
                     'TAX': 'TAX',
                     None: ''}

        arg_dict = {
            'Garden State MLS': ["//h1[normalize-space()='Garden State MLS Notices']",
                                 "//div[@id='navigation-container']", "//h2[normalize-space()='FIND WHAT YOU NEED']"],
            'Advanced Search': ["//header[@class='gsmls_header']//h2[1]",
                                "//div[@id='adv-uncheck-all']", "//li[normalize-space()='* ® County']"],
            'Results': [f"//h2[normalize-space()='{prop_dict[property_type]} Results']",
                        "//div[@id='sub-navigation-container']", "//table[@class='df-table sticky sticky-gray']"],
            'Download': ["//h2[normalize-space()='Download']",
                         "//div[@id='sub-navigation-container']", "//form[@id='downloadoption']//section[1]//div[1]//div[1]"],
            'Media Page': ["//div[@id='menu-selectBox']//div[2]", "//div[@class='imagesReportTitle']",
                           f'//*[@id="{prop_id}"]/div/form']
        }

        if page_name in ['Garden State MLS', 'Advanced Search', 'Results', 'Download']:
            # Wait 1
            WebDriverWait(driver_var, 10).until(
                EC.text_to_be_present_in_element((By.XPATH, arg_dict[page_name][0]), page_name))
            # Wait 2
            WebDriverWait(driver_var, 10).until(
                EC.visibility_of_element_located((By.XPATH, arg_dict[page_name][1])))
            # Wait 3
            WebDriverWait(driver_var, 10).until(
                EC.visibility_of_element_located((By.XPATH, arg_dict[page_name][2])))

        elif page_name == 'Media Page':
            assert window_id == driver_var.current_window_handle
            # pprint(driver_var.page_source)
            # Wait 1
            WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, arg_dict[page_name][0])))
            # # Wait 2
            WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, arg_dict[page_name][1])))
            # Wait 3
            WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, arg_dict[page_name][2])))

    def find_cities(self, county_id, page_source):
        """

        :param county_id
        :param page_source:
        :return:
        """

        value_pattern = re.compile(r'title="(\d{4,5}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, 'html.parser')
        target = soup.find('div', {"id": "town1"})
        target_contents = target.find_all('div', {'class': 'selection-item'})
        self.municipalities.setdefault(county_id, {})

        for i in target_contents:
            # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = value_pattern.search(str(i))
            self.municipalities[county_id][target_search.group(1)] = target_search.group(2)

    def find_counties(self, page_source):
        """

        :param page_source:
        :return:
        """
        attribute_pattern = re.compile(r'(\d{2})\s-\s(\w+)')
        title_pattern = re.compile(r'title="(\d{2,3}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, 'html.parser')
        target_contents = soup.find_all('label', {'title': attribute_pattern})

        for i in target_contents:
            # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = title_pattern.search(str(i))
            if target_search.group(2) == 'Other':
                continue
            self.counties[target_search.group(1)] = target_search.group(2)

    @staticmethod
    def find_link_index(button, page_source):

        soup = BeautifulSoup(page_source, 'html.parser')
        nav_menu = soup.find('div', {'id': 'sub-navigation-container'})

        for idx, a_link in enumerate(nav_menu.find_all('a')):
            text = a_link.get_text().strip()
            if text == button:
                return idx + 1


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
    def kill_logger(logger_var, file_handler, console_handler):

        logger_var.removeHandler(file_handler)
        logger_var.removeHandler(console_handler)
        logging.shutdown()

    @staticmethod
    def last_day_of_month(month):

        target = calendar.month(2023, month)
        # Isolate the last week of the month, then the last day of that week
        return target.split('\n')[-2].split(' ')[-1]

    def load_metadata(self):

        metadata = pd.read_sql_table('gsmls_event_log', self.engine)
        last_row = metadata.shape[0] - 1

        if metadata.empty:
            pass
        else:

            self.last_scraped_qtr = metadata.loc[last_row, 'quarter']
            self.last_scraped_year = metadata.loc[last_row, 'year_']
            self.last_scraped_county = metadata.loc[last_row, 'county']
            self.last_scraped_muni = metadata.loc[last_row, 'municipality']
            self.finished = metadata.loc[last_row, 'finished']
            self.last_scraped_property_type = metadata.loc[last_row, 'property_type']

    @staticmethod
    def login(website, driver_var):
        """

        :param website:
        :param driver_var:
        :return:
        """
        username, _, pw = GSMLS.get_us_pw(website)

        if website == 'GSMLS':
            gsmls_id = driver_var.find_element(By.ID, 'usernametxt')
            gsmls_id.click()
            gsmls_id.send_keys(username)
            password = driver_var.find_element(By.ID, 'passwordtxt')
            password.click()
            password.send_keys(pw)
            login_button = driver_var.find_element(By.ID, 'login-btn')
            login_button.click()
            time.sleep(1.5)  # Built-in latency
            page_results = driver_var.page_source
            soup = BeautifulSoup(page_results, 'html.parser')

            # Check if there's a duplicate session running. If so, terminate it
            duplicate = soup.find('input',
                    {'class':'gs-btn-submit-sh gs-btn-submit-two tertiary-color ps-tertiary-color fs14 popup_button_0'})
            if type(duplicate) == bs4.element.Tag:
                terminate_duplicate_session = WebDriverWait(driver_var, 5).until(
                                EC.presence_of_element_located((By.XPATH, '//*[@id="alert_popup"]/div/div[2]/input[1]')))
                terminate_duplicate_session.click()

            time.sleep(1)
            page_results = driver_var.page_source
            soup = BeautifulSoup(page_results, 'html.parser')
            notice_msg = soup.find('div', {'id': 'notice-box'})

            # Check if there's a GSMLS popup notice. If so, close the message
            if type(notice_msg) == bs4.element.Tag:
                try:
                    ok_button = WebDriverWait(driver_var, 1).until(
                                EC.presence_of_element_located((By.XPATH, "//input[@value='OK']")))
                    ok_button.click()
                except TimeoutException:
                    pass


    @staticmethod
    def no_results(driver_var):

        # page_source = driver_var.page_source
        # soup = BeautifulSoup(page_source, 'html.parser')
        # message_box = soup.find('div', {'id': 'message-box-container', 'class': 'active-container'})

        # if message_box:
        no_results_found = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="message-box"]/div[2]/input')))
        no_results_found.click()
        time.sleep(1)  # Built-in latency

    @staticmethod
    def page_criteria(timeframe, driver_var):

        # Locate and click the Status tab
        x_path = '//*[@id="advance-search-fields"]/li[2]'
        status_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path)))
        status_tab.click()
        time.sleep(1)

        uncheck_all = driver_var.find_element(By.ID, "adv-uncheck-all")
        uncheck_all.click()  # Step 2: Uncheck unwanted statuses

        if timeframe == 'historic':
            # Click the radio symbols which return historic data
            target_status = ['SD', 'WD', 'XD']

            for target in target_status:
                status = driver_var.find_element(By.ID, target)
                status.click()  # Step 3: Check the sold status

        elif timeframe == 'current':
            # Click the radio symbols which return historic data
            target_status = ['S', 'W', 'X']

            for target in target_status:
                status = driver_var.find_element(By.ID, target)
                status.click()  # Step 3: Check the sold status

    def quarterly_sales_res(self, driver_var, **kwargs):
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
        qtr = kwargs['Qtr']
        date_range = kwargs['Dates']
        property_types = ['RES', 'MUL', 'LND', 'RNT', 'TAX']

        with tqdm(total=len(property_types), desc='Property Types', colour='magenta') as properties_bar:
            for type_ in property_types:

                if self.last_scraped_property_type is not None:
                    if type_ != self.last_scraped_property_type:
                        properties_bar.update(1)
                        time.sleep(0.2)
                        continue
                    else:
                        self.last_scraped_property_type = None
                        kwargs['Property_Type'] = type_

                # Click the property type in the dropdown menu
                GSMLS.click_property_type(type_, driver_var)
                # Set the page criteria
                # Make the timeframe an instance var that can dynamically change
                GSMLS.page_criteria('historic', driver_var)
                # Set the dates
                GSMLS.set_dates(date_range, type_, driver_var)
                # If this is rent the property styles
                if type_ == 'RNT':
                    GSMLS.res_property_styles(driver_var)

                with tqdm(total=len(self.municipalities.keys()), desc='Counties', colour='yellow') as counties_bar:
                    for county, municipality in self.municipalities.items():

                        if self.last_scraped_county is not None:
                            if int(county) != self.last_scraped_county:
                                counties_bar.update(1)
                                time.sleep(0.2)
                                continue
                            else:
                                self.last_scraped_county = None


                        GSMLS.click_target_tab('County', driver_var)
                        GSMLS.set_county(2, county, driver_var)  # Set the county
                        GSMLS.click_target_tab('Town', driver_var)

                        with tqdm(total=len(municipality.keys()), desc='Municipalities', colour='green', position=1) as muni_bar:
                            for city_id, city_name in municipality.items():

                                if self.last_scraped_muni is not None:
                                    if city_name != self.last_scraped_muni:
                                        muni_bar.update(1)
                                        time.sleep(0.2)
                                        continue
                                    else:
                                        self.last_scraped_muni = None

                                GSMLS.set_city(2, city_id, driver_var)  # Set the city
                                GSMLS.show_results(driver_var)  # Click the Show Results button
                                time.sleep(2)
                                page_results1 = driver_var.page_source

                                self.download_log['Year_'].append(kwargs['Year'])
                                self.download_log['Quarter'].append(qtr)
                                self.download_log['County'].append(county)
                                self.download_log['Municipality'].append(city_name)
                                self.download_log['Initiated'].append('Yes')
                                self.download_log['Finished'].append('No')
                                self.download_log['Rows_Produced'].append(0)
                                self.download_log['Date_Produced'].append(str(datetime.now()))
                                self.download_log['Property_Type'].append(type_)

                                if "Your search returned 0 records" in str(page_results1):
                                    # No results found
                                    self.download_log['Results_Found'].append('No')
                                    self.download_log['Finished'][-1] = 'Yes'
                                    GSMLS.no_results(driver_var)
                                    muni_bar.update(1)
                                    logger.info(f'There is no GSMLS sales data available for {city_name}')
                                    GSMLS.click_target_tab('Town', driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)
                                elif "Only first 500 records will be displayed." in str(page_results1):
                                    # Too many results were found, split the search dates
                                    self.split_search_dates(qtr, date_range, type_, city_name, county, driver_var, **kwargs)
                                    GSMLS.click_target_tab('Town', driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)

                                else:
                                    # Results were found
                                    # Sales file will be requested and additional data will be added
                                    # and formatted before being produced to Apache Kafka
                                    self.download_log['Results_Found'].append('Yes')
                                    filename = self.download_sales_data(city_name, self.counties[county], qtr,
                                                                        kwargs['Year'], kwargs['Property_Type'], driver_var, kwargs['Main_Window'])
                                    time.sleep(1.5)  # Built-in latency to allow page to load
                                    additional_info = GSMLS.format_data_for_kafka(driver_var, kwargs['Year'],
                                                                                  city_name, kwargs['Property_Type'], logger)
                                    self.publish_data_2kafka(filename, additional_info, **kwargs)
                                    self.download_log['Finished'][-1] = 'Yes'
                                    muni_bar.update(1)
                                    GSMLS.exit_results_page(driver_var)
                                    GSMLS.click_target_tab('Town', driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)

                        GSMLS.click_target_tab('County', driver_var)
                        GSMLS.set_county(2, county, driver_var)  # Set the county
                        counties_bar.update(1)

                properties_bar.update(1)

    @staticmethod
    def page_search(search_type: int, page_results, driver_var):
        """
        Click the quick search menu to start scraping the sales data
        :param search_type:
        :param page_results:
        :param driver_var:
        :return:
        """

        # Click the Search tab
        soup = BeautifulSoup(page_results, 'html.parser')
        target = soup.find('li', {"class": "nav-header", "id": "2"})
        submenu_id = target.find('a', {"href": "#", "class": "has-submenu"})['id']
        main_search = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, submenu_id)))
        main_search.click()

        # Click the Advanced Search sub-menu
        quicksearch_menu = target.find('li', {"id": f"2_{search_type}"})
        quicksearch_menu_id = quicksearch_menu.find('a', {"href": "#", "class": "disabled has-submenu"})['id']
        extended_search_menu = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, quicksearch_menu_id)))
        extended_search_menu.click()

        # Click the XPY option which allows the access of RES, MUL and LND sales data
        xpy_search = WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located((By.ID, f'2_{search_type}_1')))
        xpy_search.click()

    @staticmethod
    def res_property_styles(driver_var):
        """

        :param driver_var:
        :return:
        """

        # Locate and click the County tab
        x_path = '//*[@id="advance-search-fields"]/li[8]'
        property_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path)))
        property_tab.click()
        time.sleep(1)

        for type_ in ['RES', 'MUL', 'LND']:
            driver_var.find_element(By.ID, type_).click()

    @staticmethod
    def return_target_columns(df, ptypes: str):

        if ptypes == 'RES':

            columns = ['MLSNUM', 'STATUS_SHORT', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                       'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'DAYSONMARKET', 'ORIGLISTPRICE',
                       'LISTPRICE', 'SALESPRICE', 'SP/LP%', 'LOANTERMS_SHORT', 'ROOMS', 'BEDS','BATHSTOTAL',
                       'SQFTAPPROX', 'ACRES', 'LOTSIZE', 'ASSESSAMOUNTBLDG', 'ASSESSAMOUNTLAND', 'ASSESSTOTAL','SUBPROPTYPE',
                       'STYLEPRIMARY_SHORT', 'STYLE_SHORT', 'SUBDIVISION', 'TAXAMOUNT', 'TAXRATE', 'TAXYEAR','YEARBUILT',
                       'LISTDATE', 'PENDINGDATE', 'ANTICCLOSEDDATE', 'CLOSEDDATE', 'EXPIREDATE', 'WITHDRAWNDATE', 'OWNERSHIP_SHORT',
                       'EASEMENT_SHORT', 'PARKNBRAVAIL','DRIVEWAYDESC_SHORT', 'GARAGECAP', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                       'COOLSYSTEM_SHORT', 'WATER_SHORT', 'UTILITIES_SHORT', 'EXTERIOR_SHORT', 'FIREPLACES', 'FLOORS_SHORT',
                       'POOL_SHORT', 'ROOF_SHORT', 'SEWER_SHORT', 'SIDING_SHORT', 'BASEMENT_SHORT', 'BASEDESC_SHORT',
                       'FLOODZONE', 'ZONING', 'APPFEE',  'ASSOCFEE', 'COMPBUY', 'COMPSELL', 'COMPTRANS', 'LISTTYPE_SHORT',
                       'OFFICELIST', 'OFFICESELL', 'OFFICESELLNAME', 'AGENTSELLNAME', 'OWNERNAME', 'AGENTLIST', 'AGENTSELL',
                       'REMARKSAGENT', 'REMARKSPUBLIC', 'SHOWSPECIAL', 'BUSRELATION_SHORT']

            return df[columns]

        elif ptypes == 'MUL':

            columns = ['MLSNUM', 'STATUS_SHORT', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                       'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'DAYSONMARKET', 'ORIGLISTPRICE',
                       'LISTPRICE', 'SALESPRICE', 'SP/LP%', 'LOANTERMS_SHORT', 'NUMUNITS', 'ROOMS', 'BEDS','BATHSTOTAL',
                       'SQFTBLDG', 'ACRES', 'LOTSIZE', 'ASSESSAMOUNTBLDG', 'ASSESSAMOUNTLAND', 'ASSESSTOTAL',
                       'UNITSTYLE_SHORT', 'SUBDIVISION', 'TAXAMOUNT', 'TAXRATE', 'TAXYEAR','YEARBUILT',
                       'INCOMEGROSSOPERATING', 'EXPENSEOPERATING', 'INCOMENETOPERATING', 'EXPENSESINCLUDE_SHORT', 'UNIT1BEDS',
                       'UNIT1BATHS', 'UNIT1ROOMS', 'UNIT1OWNERTENANTPAYS_SHORT', 'UNIT2BEDS', 'UNIT2BATHS', 'UNIT2ROOMS',
                       'UNIT2OWNERTENANTPAYS_SHORT', 'UNIT3BEDS', 'UNIT3BATHS', 'UNIT3ROOMS', 'UNIT3OWNERTENANTPAYS_SHORT',
                       'UNIT4BEDS', 'UNIT4BATHS', 'UNIT4ROOMS', 'UNIT4OWNERTENANTPAYS_SHORT',
                       'LISTDATE', 'PENDINGDATE', 'ANTICCLOSEDDATE', 'CLOSEDDATE', 'EXPIREDATE', 'WITHDRAWNDATE', 'OWNERSHIP_SHORT',
                       'EASEMENT_SHORT', 'PARKNBRAVAIL','DRIVEWAYDESC_SHORT', 'GARAGECAP', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                       'COOLSYSTEM_SHORT', 'WATER_SHORT', 'UTILITIES_SHORT', 'EXTERIOR_SHORT', 'FIREPLACES', 'FLOORS_SHORT',
                       'POOL_SHORT', 'ROOF_SHORT', 'SEWER_SHORT', 'SIDING_SHORT', 'BASEMENT_SHORT', 'BASEDESC_SHORT',
                       'FLOODZONE', 'ZONING', 'APPFEE',  'ASSOCFEE', 'COMPBUY', 'COMPSELL', 'COMPTRANS', 'LISTTYPE_SHORT',
                       'OFFICELIST', 'OFFICESELL', 'OFFICESELLNAME', 'AGENTSELLNAME', 'OWNERNAME', 'AGENTLIST', 'AGENTSELL',
                       'REMARKSAGENT', 'REMARKSPUBLIC', 'SHOWSPECIAL', 'BUSRELATION_SHORT']

            return df[columns]

        elif ptypes == 'LND':

            columns = ['MLSNUM', 'STATUS_SHORT', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                       'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'DAYSONMARKET', 'ORIGLISTPRICE',
                       'LISTPRICE', 'SALESPRICE', 'SP/LP%', 'LOANTERMS_SHORT', 'NUMLOTSTS',
                       'ACRES', 'LOTSIZE', 'ASSESSAMOUNTBLDG', 'ASSESSAMOUNTLAND', 'ASSESSTOTAL',
                       'SUBDIVISION', 'TAXAMOUNT', 'TAXRATE', 'TAXYEAR','YEARBUILT',
                       'LISTDATE', 'PENDINGDATE', 'ANTICCLOSEDDATE', 'CLOSEDDATE', 'EXPIREDATE', 'WITHDRAWNDATE',
                       'FLOODZONE', 'ZONINGDESC_SHORT', 'BUILDINGSINCLUDED_SHORT', 'CURRENTUSE_SHORT', 'DEVRESTRICT_SHORT', 'DEVSTATUS_SHORT',
                       'EASEMENT_SHORT', 'IMPROVEMENTS_SHORT', 'LOTDESC_SHORT', 'PERCTEST_SHORT', 'ROADFRONTDESC_SHORT',
                       'ROADSURFACEDESC_SHORT', 'SERVICES_SHORT', 'SEWERINFO_SHORT', 'SITEPARTICULARS_SHORT', 'SOILTYPE_SHORT',
                       'TOPOGRAPHY_SHORT', 'WATERINFO_SHORT', 'COMPBUY', 'COMPSELL', 'COMPTRANS', 'LISTTYPE_SHORT',
                       'OFFICELIST', 'OFFICESELL', 'OFFICESELLNAME', 'AGENTSELLNAME', 'OWNERNAME', 'AGENTLIST',
                       'AGENTSELL', 'REMARKSAGENT', 'REMARKSPUBLIC', 'SHOWSPECIAL', 'BUSRELATION_SHORT']

            return df[columns]

        elif ptypes == 'RNT':

            columns = ['MLSNUM', 'STATUS_SHORT', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                       'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'DAYSONMARKET', 'RENTPRICEORIG',
                       'LP', 'RENTMONTHPERLSE', 'RP/LP%', 'LEASETERMS_SHORT','ROOMS', 'BEDS','BATHSTOTAL',
                       'SQFTAPPROX', 'SUBDIVISION', 'YEARBUILT', 'PROPERTYTYPEPRIMARY_SHORT', 'PROPSUBTYPERN',
                       'LOCATION_SHORT', 'PRERENTREQUIRE_SHORT', 'OWNERPAYS_SHORT', 'TENANTPAYS_SHORT',
                       'TENANTUSEOF_SHORT', 'RENTINCLUDES_SHORT', 'RENTTERMS_SHORT', 'LENGTHOFLEASE', 'AVAILABLE_SHORT',
                       'APPLIANCES_SHORT', 'LAUNDRYFAC',
                       'FURNISHINFO_SHORT', 'PETS_SHORT', 'PARKNBRAVAIL','DRIVEWAYDESC_SHORT',
                       'BASEMENT_SHORT', 'BASEDESC_SHORT', 'GARAGECAP', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                       'COOLSYSTEM_SHORT', 'WATER_SHORT', 'UTILITIES_SHORT', 'FLOORS_SHORT', 'POOL_SHORT', 'SEWER_SHORT',
                       'TENLANDCOMM_SHORT', 'REMARKSAGENT', 'REMARKSPUBLIC', 'SHOWSPECIAL']

            return df[columns]

        elif ptypes == 'TAX':

            columns = ['AUTOROW', 'CITYCODE','BLOCKID', 'BLOCKSUFFIX', 'LOT', 'LOTSUFFIX', 'PARCEL_NO', 'MCR', 'MAP',
                       'LOCNUM', 'LOCDIR', 'LOCSTREET', 'LOCMODE', 'LOCCITY', 'LOCSTATE', 'LOCZIP', 'PROPERTYDESC',
                       'PROPERTYUSECODE', 'EQVALUE', 'BANKCODE', 'SALEDATE', 'SALEPRICE', 'TAXES', 'TAXYR', 'RATE', 'RATIO', 'RATIOYR',
                       'TOTALASSESSMENT', 'ASSESSMENT2', 'ASSESSMENT1', 'YEARBUILT', 'BUILDINGDESC', 'BUILDINGCLASSCODE', 'ACRES',
                       'ADDITIONALLOTS', 'DEEDBOOK', 'DEEDPAGE', 'OWNER', 'OWNERS','MAILNUM', 'MAILDIR', 'MAILSTREET',
                       'MAILMODE', 'MAILCITY', 'MAILSTATE', 'MAILZIP', 'PRIOROWNER', 'PRIORSALEAMT', 'PRIORSALEDATE',
                       'PRIORDEEDBOOK', 'PRIORDEEDPAGE', 'DATEMODIFIED']

            return df[columns]

    @staticmethod
    def format_data_for_kafka(driver_var, year, municipality, prop_type, logger):

        sold_listings_dictionary = {
            'MLSNUM': [],
            'LATITUDE': [],
            'LONGITUDE': [],
            'IMAGES': []
        }

        # Use a checkpoint to make sure page is loaded
        GSMLS.explicit_page_load('Results', driver_var, property_type=prop_type)

        # Step 1: Acquire the page source and find the main table holding the property information
        page_source = driver_var.page_source
        latlong_pattern = re.compile(r'navigate\((.*),(.*)\)')
        soup = BeautifulSoup(page_source, 'html.parser')
        first_table = soup.find('table', {'class': 'df-table sticky sticky-gray'})
        main_table = first_table.find('tbody')
        sold_listings = main_table.find_all('tr')
        prop_id, first_media_idx = GSMLS.first_media_link(sold_listings, sold_listings_dictionary)

        main_window = driver_var.current_window_handle

        # Step 2: Scrape the links for all high resolution images associated with each property
        if type(first_media_idx) is int:
            try:
                GSMLS.scrape_image_links(sold_listings_dictionary, driver_var, first_media_idx, prop_id)
                # Step 3: Switch to main property table window after scraping images
                driver_var.switch_to.window(main_window)
            except TimeoutException:
                # Visibility of the 'imagesReportTitle' wasn't found. How do I fix this?
                logger.warning(f'The image urls for {municipality} in {year} were not scraped')
                pass

        # Step 4: Loop through all rows of the table to get target information
        # We do not include the last index because it will result in an error
        for result in sold_listings:

            sold_listings_dictionary['MLSNUM'].append(result.find('td', {'class': 'mlnum'}).a.get_text().strip())
            address = result.find('td', {'class': 'address'}).find_all('a')[-1]
            latlong = latlong_pattern.search(str(address))
            sold_listings_dictionary['LATITUDE'].append(latlong.group(1))
            sold_listings_dictionary['LONGITUDE'].append(latlong.group(2))

        return sold_listings_dictionary

    @staticmethod
    def first_media_link(bs4_obj, soldlistings):
        """
        Find the first media link in the searches to scrape the hi res images
        :param bs4_obj:
        :param soldlistings:
        :return:
        """

        for idx, item in enumerate(bs4_obj):

            try:
                media_link = item.find('td', {'class': 'media'}).a
                if media_link.get_text().strip() != '':
                    prop_id = item.find('td', {'class': 'item-number'})['data-seq']
                    return prop_id, idx + 1
                elif media_link.get_text().strip() == '':
                    soldlistings['IMAGES'].append('None')
                    continue


            except AttributeError:
                soldlistings['IMAGES'].append('None')
                continue

        return None, None

    def publish_data_2kafka(self, xls_file_name: str, soldlistings: dict, **kwargs):

        base_path = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'
        kafka_data_prod = kwargs['data-producer']

        if GSMLS.download_complete(xls_file_name):
            sold_df = pd.read_excel(os.path.join(base_path, xls_file_name + '.xls'), engine='xlrd')
            sold_df.columns = sold_df.columns.str.upper()
            sold_df = GSMLS.return_target_columns(sold_df, kwargs['Property_Type'])
            sold_df = sold_df.astype({'MLSNUM': 'string'})

            # Merge the Latitude and Longitude data from the image df to the sold listings df
            if kwargs['Property_Type'] in ['RES', 'MUL', 'LND']:

                geo_data = pd.DataFrame({'MLSNUM': soldlistings['MLSNUM'], 'LATITUDE': soldlistings['LATITUDE'],
                                         'LONGITUDE': soldlistings['LONGITUDE'], 'IMAGES': soldlistings['IMAGES']})

                target_df = pd.merge(sold_df, geo_data, on='MLSNUM')
                target_df['MLS'] = 'GSMLS'
                target_df['QTR'] = kwargs['Qtr']
                target_df['CONDITION'] = 'Unknown'

                if kwargs['Property_Type'] in ['RES', 'MUL']:
                    image_df = target_df[['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                       'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'CONDITION', 'IMAGES']]
                    image_df = image_df.to_json(orient='split', date_format='iso')
                    kafka_data_prod.send('prop_images', image_df)
                    kafka_data_prod.flush()

            elif kwargs['Property_Type'] == 'RNT':
                target_df = sold_df
                target_df['QTR'] = kwargs['Qtr']
                target_df['CONDITION'] = 'Unknown'
            elif kwargs['Property_Type'] == 'TAX':
                target_df = sold_df

            # Send to Kafka
            self.rows_counted += len(sold_df)
            self.download_log['Rows_Produced'][-1] = len(sold_df)
            target_df = target_df.to_json(orient='split', date_format='iso')

            if kwargs['Property_Type'] == 'RES':
                kafka_data_prod.send('res_properties', value=target_df)
            elif kwargs['Property_Type'] == 'MUL':
                kafka_data_prod.send('mul_properties', value=target_df)
            elif kwargs['Property_Type'] == 'LND':
                kafka_data_prod.send('lnd_properties', value=target_df)
            elif kwargs['Property_Type'] == 'RNT':
                kafka_data_prod.send('rnt_properties', value=target_df)
            elif kwargs['Property_Type'] == 'TAX':
                kafka_data_prod.send('tax_properties', value=target_df)

            kafka_data_prod.flush()
            self.download_log['Date_Produced'][-1] = (str(datetime.now()))

            GSMLS.sendfile2trash(xls_file_name)

    def save_metadata(self):

        metadata = pd.DataFrame(self.download_log)
        metadata.columns = metadata.columns.str.lower()
        metadata.to_sql('gsmls_event_log', con=self.engine, if_exists='append', index=False)

    @staticmethod
    def scrape_image_links(dict_var, driver_var, link_var, prop_id):
        try:
            # Step 1: Find the respective media link and open it
            # print('Before media:', driver_var.window_handles)
            current_windows = driver_var.window_handles
            media_link = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, f'//*[@id="searchResult"]/main/div[1]/table/tbody/tr[{link_var}]/td[4]/a')))
            media_link.click()
            media_window = [window for window in driver_var.window_handles if window not in current_windows][-1]

            # Step 2: Switch to new media links window
            driver_var.switch_to.window(media_window)
            WebDriverWait(driver_var, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # driver_var.fullscreen_window()
            GSMLS.explicit_page_load('Media Page', driver_var, prop_id=prop_id, window_id=media_window)

            # Step 3: Scrape the listing IDs
            soup = BeautifulSoup(driver_var.page_source, 'html.parser')
            outercontainer = soup.find('div', {'id': 'outerContainer'}).find('form').find('input', {'name': 'sysIds'})
            sys_ids = outercontainer['value'].split(',')

            # Step 3: Scrape the webpage and all associated high res image links starting from the first image link
            for listing_id in sys_ids[link_var - 1:]:
                image_dictionary = {}

                GSMLS.explicit_page_load('Media Page', driver_var, prop_id=listing_id, window_id=media_window)

                soup = BeautifulSoup(driver_var.page_source, 'html.parser')
                images_list = soup.find_all('div', {'class': 'imageReportContainer'})

                if len(images_list) > 0:
                    raw_property_address = soup.find('div', {'class': 'imagesReportTitle'}).get_text(strip=True).split('•')[1].strip()
                    clean_address = GSMLS.clean_address(raw_property_address)
                    for image_num, image in enumerate(images_list):
                        # The high res image is in the value attribute of the first input tag
                        # img_link =
                        image_dictionary[f"{clean_address} - {image.img['alt']} - {image_num}"] = image.input['value']

                    dict_var['IMAGES'].append(image_dictionary)
                elif len(images_list) == 0:
                    dict_var['IMAGES'].append('None')

                if listing_id != sys_ids[-1]:
                    try:
                        # Step 4: Find 'NEXT' link to cycle through the list of property pictures
                        next_button = WebDriverWait(driver_var, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Next']")))
                        next_button.click()
                    except UnexpectedAlertPresentException:
                        # This alert is raised when the 'Next' button is clicked and there are no more properties left in the list
                        alert = Alert(driver_var)
                        alert.accept()
                        driver_var.close()
                else:
                    driver_var.close()

        except WebDriverException as w:
            print(f'{w}')
        except BaseException as b:
            print(f'{b}')

    def scrape_municipalities(self, county_id, driver_var):

        GSMLS.set_county(1, county_id, driver_var)
        time.sleep(1)  # Latency period added in order to load and scrape city names
        self.find_cities(county_id, driver_var.page_source)
        GSMLS.set_county(1, county_id, driver_var)

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
    def sendfile2trash(xls_file_name: str):

        send2trash.send2trash(os.path.join('C:\\Users\\Omar\\Desktop\\Selenium Temp Folder', xls_file_name + '.xls'))

    @staticmethod
    def set_city(search_type, city_id_var, driver_var):

        if search_type == 1:
            click_city = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, city_id_var)))
            click_city.click()
        else:
            # The ID variable should have "town" in front of it
            click_city = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "town" + str(city_id_var))))
            click_city.click()

    @staticmethod
    def set_county(search_type, county_id_var, driver_var):

        if search_type == 1:
            click_county = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, county_id_var)))
            click_county.click()
        else:
            # The ID variable should have "county" in front of it
            click_county = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "county" + str(county_id_var))))
            click_county.click()

    @staticmethod
    def set_dates(date_range, type_, driver_var):

        ids_list = [('CLOSEDDATEmin', 'CLOSEDDATEmax'), ('EXPIREDATEmin', 'EXPIREDATEmax'),
                    ('WITHDRAWNDATEmin', 'WITHDRAWNDATEmax')]
        rent_ids = [('RENTEDDATEmin', 'RENTEDDATEmax'), ('EXPIREDATEmin', 'EXPIREDATEmax'),
                    ('WITHDRAWNDATEmin', 'WITHDRAWNDATEmax')]
        tax_ids = [('SALEDATEmin', 'SALEDATEmax')]

        # XPath ID values which correspond to the closed, expired and withdrawn dates
        values = {'RES': [48, 66,174],
                  'MUL': [33, 47,174],
                  'LND': [31, 42,102],
                  'RNT': [142, 61, 172],
                  'TAX': [81]
                  }

        if type_ in ['RES', 'MUL', 'LND']:
            target_list = ids_list
        elif type_ == 'RNT':
            target_list = rent_ids
        else:
            target_list = tax_ids

        for value, daterangeids in zip(values[type_], target_list):
            # Locate and click the target tab
            x_path = f'//*[@id="advance-search-fields"]/li[{value}]'
            target_tab = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, x_path)))
            target_tab.click()
            time.sleep(1)

            starting_close_date = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, daterangeids[0])))
            starting_close_date.click()  # Step 5: Choose start date
            AC(driver_var).key_down(Keys.CONTROL).send_keys('A').key_up(Keys.CONTROL).send_keys(date_range[0]).perform()
            ending_close_date = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, daterangeids[1])))
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

    def split_search_dates(self, quarter: int, date_range: list, type_, city_name, county, driver_var, **kwargs):
        """

        :param quarter:
        :param date_range:
        :param type_:
        :param city_name:
        :param county:
        :param driver_var:
        :param kwargs:
        :return:
        """

        # Too many results were found, exit the alert popup and continue with script
        GSMLS.too_many_results(driver_var)

        # Parse the date range for the first and last month, as well as the year
        year = date_range[0].split('/')[-1]
        first_month = int(date_range[0].split('/')[0])
        last_month = int(date_range[0].split('/')[0])

        for month in range(first_month, last_month + 1):

            # Get the last day of the month for the target month
            last_day = GSMLS.last_day_of_month(month)
            if len(str(month)) == 1:
                start_date, end_date = f'{"0" + str(month)}/01/{year}', f'{"0" + str(month)}/{last_day}/{year}'
                GSMLS.set_dates([start_date, end_date], type_, driver_var)

            elif len(str(month)) == 2:
                start_date, end_date = f'{month}/01/{year}', f'{month}/{last_day}/{year}'
                GSMLS.set_dates([start_date, end_date], type_, driver_var)

            GSMLS.show_results(driver_var)
            self.download_log['Results_Found'].append('Yes')
            filename = self.download_sales_data(city_name, self.counties[county], quarter, kwargs['Year'], kwargs['Property_Type'], driver_var)
            additional_info = GSMLS.format_data_for_kafka(driver_var, kwargs['Year'], city_name, kwargs['Property_Type'], kwargs['logger'])
            self.publish_data_2kafka(filename, additional_info, **kwargs)
            self.download_log['Finished'][-1] = 'Yes'
            GSMLS.exit_results_page(driver_var)

    @staticmethod
    def too_many_results(driver_var):

        no_button = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.XPATH, "//input[@value='No']")))
        no_button.click()
        time.sleep(1)  # Built-in latency

    @logger_decorator
    def main(self, driver_var=None, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']
        change_count = 0
        kwargs['data-producer'] = KafkaProducer(bootstrap_servers='localhost:9092',
                                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        try:
            # Step 1: Login to the GSMLS
            GSMLS.login('GSMLS', driver_var)
            GSMLS.explicit_page_load('Garden State MLS', driver_var)
            kwargs['Main_Window'] = driver_var.current_window_handle

            # Step 2: Choose the property search type
            page_results = driver_var.page_source
            if self.municipalities == {}:
                GSMLS.page_search(2, page_results, driver_var)
                time.sleep(2)  # Build-in latency to let the page load

                # Step 3: Scrape all the county and municipality targets
                self.create_state_dictionary(driver_var)
                page_results = driver_var.page_source

            GSMLS.page_search(1, page_results, driver_var)
            GSMLS.explicit_page_load('Advanced Search', driver_var)
            # GSMLS.res_property_styles(driver_var)
            # GSMLS.page_criteria('historic', driver_var)

            # Step 4: Create the time periods for which to search for data
            years = range(1995, datetime.now().year + 1)
            with tqdm(total=len(years), desc='Years', colour='red') as year_bar:
                for year in years:

                    # Ensure that the program doesn't progress into peak hours
                    # assert datetime.now().hour < 6, 'Peak hours approaching. Ending program...'

                    if self.last_scraped_year is not None:
                        if year == self.last_scraped_year:
                            self.last_scraped_year = None
                        elif year < self.last_scraped_year:
                            year_bar.update(1)
                            time.sleep(0.2)
                            continue

                    time_periods = {
                        1 : [f'01/01/{year}', f'03/31/{year}'],
                        2 : [f'04/01/{year}', f'06/30/{year}'],
                        3 : [f'07/01/{year}', f'09/30/{year}'],
                        4 : [f'10/01/{year}', f'12/31/{year}']
                    }
                    with tqdm(total=len(time_periods), desc='Qtr', colour='blue', position=1) as quarters_bar:
                        for qtr, date_range in time_periods.items():

                            kwargs['Qtr'] = qtr
                            kwargs['Dates'] = date_range
                            kwargs['Year'] = year
                            # if year < datetime.now().year:
                            #     # change to if daterange[0] is > today's date minus 1 year: change to current
                            #     if change_count == 0:
                            #         GSMLS.page_criteria('current', driver_var)
                            #         change_count += 1
                            #     else:
                            #         pass

                            # Step 5: Scrape the quarterly sales data
                            if self.last_scraped_qtr is not None:
                                if qtr < self.last_scraped_qtr:
                                    quarters_bar.update(1)
                                    time.sleep(0.2)
                                    continue
                                else:
                                    self.last_scraped_qtr = None

                            # GSMLS.set_dates(date_range, driver_var)
                            self.quarterly_sales_res(driver_var, **kwargs)
                            quarters_bar.update(1)
                    year_bar.update(1)

            # Step 5: Sign out
            GSMLS.sign_out(driver_var)

        except TimeoutException:
            exc = sys.exception()
            logger.warning(f'{repr(traceback.format_exception(exc))}')
            logger.info('Selenium Webdriver Timeout has been experienced. Restarting program...')

        except TimeoutError:
            exc = sys.exception()
            logger.warning(f'{repr(traceback.format_exception(exc))}')
            logger.info("Property images haven't been downloaded. Restarting program...")

        except AttributeError:
            exc = sys.exception()
            logger.warning(f'{repr(traceback.format_exception(exc))}')
            logger.info('Attribute Error has been experienced. Restarting program...')

        except AssertionError as AE:
            logger.warning(f'{AE}')
            raise AssertionError

        except BaseException:
            exc = sys.exception()
            logger.warning(f'{repr(traceback.format_exception(exc))}')
            logger.info('Unknown except has occurred:')

        finally:
            driver_var.quit()
            GSMLS.kill_logger(logger, f_handler, c_handler)
            self.save_metadata()



if __name__ == '__main__':

    # save_location1 = 'C:\\Users\\jibreel.q.hameed\\Desktop\\Selenium Temp Folder'
    save_location2 = 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'  # May need to be changed
    edge_profile_path = 'C:\\Users\\Omar\\AppData\\Local\\Microsoft\\Edge\\User Data\\Default'
    options = Options()
    # Change this directory to the new one: ('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
    s = {"savefile.default_directory": save_location2,
         "download.default_directory": save_location2,
         "download.prompt_for_download": False}
    options.add_argument(f"user-data-dir]={edge_profile_path}")
    # options.add_experimental_option("detach", True)
    options.add_experimental_option("prefs", s)
    # options.add_argument("--headless=new")
    website = 'https://mls.gsmls.com/member/'
    driver = webdriver.Edge(service=Service(), options=options)

    obj = GSMLS()
    quit_program = False

    while quit_program is False:
        # Create the driver to automate GSMLS Server Requests

        try:
            driver.maximize_window()
            driver.get(website)
            obj.main(driver)

        except AssertionError:
            quit_program = True
            # Send a text message saying the program has been completed and summarize results

        finally:
            # Create the driver to automate GSMLS Server Requests
            driver = webdriver.Edge(service=Service(), options=options)
            obj.load_metadata()


