import json
import re
import selenium.common.exceptions
import send2trash
import time
import random
import os
import calendar
import bs4.element
import datetime
import pandas as pd
import sys, traceback
import logging
from dotenv import load_dotenv
from utility_func import (
    create_sql_engine,
    create_kafka_producer,
    create_selenium_webdriver,
)
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.auto import trange
from datetime import datetime
from datetime import timedelta
from utility_func import logger_decorator, get_us_pw
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
from selenium.common.exceptions import UnexpectedAlertPresentException
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.errors import KafkaTimeoutError
from kafka.errors import MessageSizeTooLargeError


class GSMLS:

    def __init__(self, remote=True, timeframe="current"):
        load_dotenv("/opt/airflow/.env")
        self.remote = remote
        self.counties = {}
        self.municipalities = {}
        self.window_ids = {}
        self.rows_counted = {"RES": 0, "MUL": 0, "LND": 0, "RNT": 0, "TAX": 0}
        self.download_log = GSMLS.create_download_log()
        self.engine = create_sql_engine("gsmls", remote=True)
        self.last_scraped_qtr = None
        self.last_scraped_year = None
        self.last_scraped_county = None
        self.last_scraped_muni = None
        self.last_scraped_property_type = None
        self.start_date = None
        self.finished = None
        self.timeframe = timeframe
        self.load_metadata(first_run="Yes")

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    def assign_timeframe(self, year):

        if year == datetime.now().year - 1:
            self.timeframe = "mixed"
        elif year == datetime.now().year:
            pass
        else:
            self.timeframe = "historic"

        return self.create_timeframe_dict(year)

    @staticmethod
    def clean_address(address, streetnum=None, zipcode=None):

        if (streetnum is not None) and (zipcode is not None):

            if "." in streetnum:
                streetnum = streetnum.split(".")[0]

            if zipcode[0] != "0" and len(zipcode) == 4:
                zipcode = "0" + zipcode

            return f"{streetnum} {address} {zipcode}"

        else:
            target = address.split(",")
            raw_address = " ".join(target[0].rstrip("*").strip().split(" "))
            city = target[1].rstrip("*").strip()
            raw_address = " ".join([i.strip() for i in raw_address.split("\xa0")])
            clean_address = ", ".join([raw_address, city])

            return clean_address

    @staticmethod
    def click_target_tab(target_name, prop_type, driver_var):

        target_dict = {"County": 1, "Status": 2, "Town": 3, "Property_Type": 8}

        GSMLS.explicit_page_load("Target Tabs", driver_var)

        # Locate and click the County tab
        if prop_type == "TAX" and target_name == "Town":
            x_path = f'//*[@id="advance-search-fields"]/li[2]'
        else:
            x_path = f'//*[@id="advance-search-fields"]/li[{target_dict[target_name]}]'

        property_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path))
        )
        property_tab.click()
        time.sleep(1)

    @staticmethod
    def click_property_type(type_, driver_var):

        # Locate and click the property type in the Advanced Search
        x_path = f"//select[@id='ptype']"
        property_menu = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path))
        )
        property_menu.click()
        choice_path = f"//option[@value='{type_}']"
        WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, choice_path))
        ).click()
        time.sleep(1)

    @staticmethod
    def create_download_log():

        clean_log = {
            "Year_": [],
            "Quarter": [],
            "County": [],
            "Municipality": [],
            "Initiated": [],
            "Results_Found": [],
            "Finished": [],
            "Rows_Produced": [],
            "Start_Date": [],
            "Date_Produced": [],
            "Property_Type": [],
        }

        return clean_log

    def create_state_dictionary(self, driver_var):

        results = driver_var.page_source
        self.find_counties(results)

        print("Preparing State Dictionary...")
        for _, county_id in zip(
            trange(len(self.counties.keys()), desc="Counties"), self.counties.keys()
        ):
            self.scrape_municipalities(county_id, driver_var)

        print("State Dictionary completed. Data will be scraped shortly...")

    def create_timeframe_dict(self, year):

        if self.timeframe == "historic":
            return {1234: [f"01/01/{year}", f"12/31/{year}"]}

        elif self.timeframe in ["current", "mixed"]:
            # Get latest scraped date
            start_date_list = self.start_date.strftime("%Y-%m-%d %H:%M:%S").split("-")
            stop_date_list = datetime.now().date().strftime("%Y-%m-%d").split("-")
            start_month, start_day = start_date_list[1], start_date_list[2]
            stop_month, stop_day, stop_year = (
                stop_date_list[1],
                stop_date_list[2],
                stop_date_list[0],
            )

            # Start scraping date from the day after the last scraped day
            return {
                1234: [
                    f"{start_month}/{start_day}/{year}",
                    f"{stop_month}/{stop_day}/{stop_year}",
                ]
            }

    @staticmethod
    def download_complete(filename):

        download_folder = "C:\\Users\\Omar\\Desktop\\Selenium Temp Folder"
        for _ in range(1, 16):
            abspath_ = os.path.join(download_folder, filename + ".xls")
            if os.path.exists(abspath_):
                return True
            else:
                time.sleep(0.5)

        return False
        # raise TimeoutError(f'{filename} did not download in a timely manner')

    @staticmethod
    def download_error(driver_var, logger):

        # Search for the message box div and check if the container is active
        try:
            WebDriverWait(driver_var, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # WebDriverWait(driver_var, 5).until(
            #     EC.presence_of_element_located((By.XPATH, "//div[@id='message-box-container']")))
            WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[@class='popup_inner']")
                )
            )
            WebDriverWait(driver_var, 5).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//h2[normalize-space()='Error']")
                )
            )
        except TimeoutException:
            pass

        soup = BeautifulSoup(driver_var.page_source, "html.parser")
        # message_box_active = soup.find('div', {'id': 'message-box-container', 'class': 'active-container'})
        message_box_active = soup.find(
            "div", {"id": "alert_popup", "class": "popup_outer active-container"}
        )

        if message_box_active is not None:
            message_type = message_box_active.find(
                "h2", {"class": "message-title"}
            ).get_text()
            box_message = message_box_active.find("div", {"class": "message"})
            message_text = box_message.get_text()

            if message_type == "Error":
                logger.warning(f"GSMLS Server Error Occurred: {message_text}")

                # Close the message box and close page
                WebDriverWait(driver_var, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@value='Ok']"))
                ).click()
                GSMLS.explicit_page_load("Server Error", driver_var)
                WebDriverWait(driver_var, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//a[@id='closect']"))
                ).click()

                return True

        else:
            return False

    @staticmethod
    def download_sales_data(
        city_name, county_name, qtr, year, prop_type, driver_var, window_id, logger
    ):

        GSMLS.explicit_page_load("Results", driver_var, property_type=prop_type)

        page_source = driver_var.page_source

        # Check all items to be downloaded
        check_all_results = WebDriverWait(driver_var, 30).until(
            EC.presence_of_element_located((By.ID, "checkall"))
        )
        check_all_results.click()

        # Locate the download button and click
        download_idx = GSMLS.find_link_index("Download", page_source)
        download_results = driver_var.find_element(
            By.XPATH,
            f'//*[@id="sub-navigation-container"]/div/nav[1]/a[{download_idx}]',
        )
        download_results.click()

        GSMLS.explicit_page_load("Download", driver_var)

        # Locate the option to download as a xls file and name it
        excel_file_input = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, "downloadfiletype3"))
        )
        excel_file_input.click()
        filename_input = driver_var.find_element(By.ID, "filename")
        filename_input.click()
        filename = (
            city_name.rstrip(".")
            + " "
            + county_name
            + " "
            + "Q"
            + str(qtr)
            + str(year)
            + f" {prop_type} Sales GSMLS"
        )
        AC(driver_var).key_down(Keys.CONTROL).send_keys("A").key_up(
            Keys.CONTROL
        ).send_keys(filename).perform()
        time.sleep(0.5)

        # Request the download and close the page
        download_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[normalize-space()='Download']")
            )
        )
        download_button.click()
        error_result = GSMLS.download_error(driver_var, logger)

        driver_var.switch_to.window(window_id)
        time.sleep(1)
        close_page = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[@id='sub-navigation-container']/div/nav[1]/a[2]")
            )
        )
        close_page.click()

        if error_result is True:
            return "Server Error"

        elif error_result is False:
            return filename

    @staticmethod
    def exit_results_page(driver_var):

        page_source = driver_var.page_source
        close_idx = GSMLS.find_link_index("Close", page_source)
        close_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    f'//*[@id="sub-navigation-container"]/div/nav[1]/a[{close_idx}]',
                )
            )
        )
        close_button.click()
        time.sleep(1.5)  # Built-in latency

    @staticmethod
    def explicit_page_load(
        page_name,
        driver_var,
        property_type=None,
        prop_id=None,
        window_id=None,
        mlsnum=None,
    ):

        prop_dict = {
            "RES": "Residential",
            "MUL": "Multi-Family",
            "LND": "Land",
            "RNT": "Rental",
            "TAX": "TAX",
            None: "",
        }

        arg_dict = {
            "Garden State MLS": [
                "//h1[normalize-space()='Garden State MLS Notices']",
                "//div[@id='navigation-container']",
                "//h2[normalize-space()='FIND WHAT YOU NEED']",
            ],
            "Advanced Search": [
                "//header[@class='gsmls_header']//h2[1]",
                "//div[@id='adv-uncheck-all']",
                "//li[normalize-space()='* ® County']",
            ],
            "Pre-Results": [
                "//header[@class='gsmls_header']//h2[1]",
                "//a[normalize-space()='Search']",
                "//li[normalize-space()='* ® County']",
                "//a[normalize-space()='Download']",
            ],
            "Results": [
                f"//h2[normalize-space()='{prop_dict[property_type]} Results']",
                "//div[@id='sub-navigation-container']",
                "//table[@class='df-table sticky sticky-gray']",
                "//a[@class='last show']",
            ],
            "Download": [
                "//h2[normalize-space()='Download']",
                "//div[@id='sub-navigation-container']",
                "//form[@id='downloadoption']//section[1]//div[1]//div[1]",
            ],
            "Property Report": [
                "//a[normalize-space()='Previous']",
                "//div[@class='report-cell title-color top-title']",
                "//div[@class='side-bar-padding']",
                f"//div[normalize-space()='{mlsnum}']",
            ],
            "Media Page": [
                "//div[@id='menu-selectBox']//div[2]",
                "//div[@class='imagesReportTitle']",
                f'//*[@id="{prop_id}"]/div/form',
            ],
            "Login": [
                "//div[@class='login-logo']",
                "//input[@id='usernametxt']",
                "//input[@id='passwordtxt']",
            ],
            "Target Tabs": [
                "//li[normalize-space()='* ® County']",
                "//li[normalize-space()='* ® Town']",
                "//li[normalize-space()='* ® Town Code']",
            ],
            "Quicksearch": [
                "//input[@id='qcksrchmlstxt']",
                "//table[@id='search-help-table']",
                "//a[@id='selcontact2']",
            ],
            "Server Error": [
                "//h2[normalize-space()='ERROR']",
                "//a[normalize-space()='HomePage']",
                "//a[@id='closect']",
            ],
        }

        if page_name in ["Garden State MLS", "Advanced Search", "Results", "Download"]:
            # Wait for the page to completely load
            WebDriverWait(driver_var, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # Wait 1
            WebDriverWait(driver_var, 15).until(
                EC.text_to_be_present_in_element(
                    (By.XPATH, arg_dict[page_name][0]), page_name
                )
            )
            # Wait 2
            WebDriverWait(driver_var, 15).until(
                EC.visibility_of_element_located((By.XPATH, arg_dict[page_name][1]))
            )
            if page_name == "Results" and property_type == "TAX":
                # Wait 3
                WebDriverWait(driver_var, 15).until(
                    EC.visibility_of_element_located(
                        (By.XPATH, "//div[@class='result-table map_adjust']")
                    )
                )
            else:
                WebDriverWait(driver_var, 15).until(
                    EC.visibility_of_element_located((By.XPATH, arg_dict[page_name][2]))
                )
            if page_name == "Results":
                # Wait 4
                # Additional check needed for Results page to make sure everything is loaded
                WebDriverWait(driver_var, 10).until(
                    EC.element_to_be_clickable((By.XPATH, arg_dict[page_name][3]))
                )

        elif page_name in ["Login", "Target Tabs"]:
            if page_name == "Target Tabs":
                WebDriverWait(driver_var, 30).until(
                    EC.presence_of_element_located((By.XPATH, arg_dict[page_name][0]))
                )

            for path_var in arg_dict[page_name]:
                WebDriverWait(driver_var, 30).until(
                    EC.visibility_of_element_located((By.XPATH, path_var))
                )

        elif page_name in ["Pre-Results", "Server Error", "Quicksearch", "SIS Results"]:
            for path_var in arg_dict[page_name]:
                WebDriverWait(driver_var, 30).until(
                    EC.visibility_of_element_located((By.XPATH, path_var))
                )

        elif page_name in ["Media Page", "Property Report"]:
            assert window_id == driver_var.current_window_handle
            # Wait 1
            WebDriverWait(driver_var, 15).until(
                EC.presence_of_element_located((By.XPATH, arg_dict[page_name][0]))
            )
            # # Wait 2
            try:
                # There are times when the ImageReportTitle Xpath doesn't show up and there no images
                for path_var in arg_dict[page_name]:
                    WebDriverWait(driver_var, 15).until(
                        EC.presence_of_element_located((By.XPATH, path_var))
                    )

            except TimeoutException:
                pass

        elif page_name in ["SIS", "RPR Login", "RPR Main", "SIS Results"]:

            try:
                for key, path_var in arg_dict[page_name].items():
                    WebDriverWait(driver_var, 15).until(
                        EC.visibility_of_element_located((By.XPATH, path_var))
                    )

            except TimeoutException:
                raise TimeoutException(f"Web element for {key} could not be found")

    def find_cities(self, county_id, page_source):
        """

        :param county_id
        :param page_source:
        :return:
        """

        value_pattern = re.compile(r'title="(\d{4,5}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, "html.parser")
        target = soup.find("div", {"id": "town1"})
        target_contents = target.find_all("div", {"class": "selection-item"})
        self.municipalities.setdefault(county_id, {})

        for i in target_contents:
            # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = value_pattern.search(str(i))
            self.municipalities[county_id][target_search.group(1)] = (
                target_search.group(2)
            )

    def find_counties(self, page_source):
        """

        :param page_source:
        :return:
        """
        attribute_pattern = re.compile(r"(\d{2})\s-\s(\w+)")
        title_pattern = re.compile(r'title="(\d{2,3}?)\s-\s(.*)"')
        soup = BeautifulSoup(page_source, "html.parser")
        target_contents = soup.find_all("label", {"title": attribute_pattern})

        for i in target_contents:
            # Strips the contents of the target counties (ie: 10 Atlantic ---> [10, Atlantic])
            target_search = title_pattern.search(str(i))
            if target_search.group(2) == "Other":
                continue
            self.counties[target_search.group(1)] = target_search.group(2)

    @staticmethod
    def find_link_index(button, page_source):

        soup = BeautifulSoup(page_source, "html.parser")
        nav_menu = soup.find("div", {"id": "sub-navigation-container"})

        for idx, a_link in enumerate(nav_menu.find_all("a")):
            text = a_link.get_text().strip()
            if text == button:
                return idx + 1

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
                media_link = item.find("td", {"class": "media"}).a
                if media_link.get_text().strip() != "":
                    prop_id = item.find("td", {"class": "item-number"})["data-seq"]
                    return prop_id, idx + 1
                elif media_link.get_text().strip() == "":
                    soldlistings["IMAGES"].append("None")
                    continue

            except AttributeError:
                soldlistings["IMAGES"].append("None")
                continue

        return None, None

    @staticmethod
    def format_data_for_kafka(driver_var, year, municipality, prop_type, logger):

        sold_listings_dictionary = {
            "MLSNUM": [],
            "LATITUDE": [],
            "LONGITUDE": [],
            "IMAGES": [],
        }
        if prop_type != "TAX":
            # Use a checkpoint to make sure page is loaded
            GSMLS.explicit_page_load("Results", driver_var, property_type=prop_type)

            # Step 1: Acquire the page source and find the main table holding the property information
            page_source = driver_var.page_source
            latlong_pattern = re.compile(r"navigate\((.*),(.*)\)")
            soup = BeautifulSoup(page_source, "html.parser")
            first_table = soup.find("table", {"class": "df-table sticky sticky-gray"})
            main_table = first_table.find("tbody")
            sold_listings = main_table.find_all("tr")
            prop_id, first_media_idx = GSMLS.first_media_link(
                sold_listings, sold_listings_dictionary
            )

            main_window = driver_var.current_window_handle

            # Step 2: Scrape the links for all high resolution images associated with each property
            if type(first_media_idx) is int:
                try:
                    GSMLS.scrape_image_links(
                        sold_listings_dictionary, driver_var, first_media_idx, prop_id
                    )
                    # Step 3: Switch to main property table window after scraping images
                    driver_var.switch_to.window(main_window)
                except TimeoutException:
                    # Visibility of the 'imagesReportTitle' wasn't found. How do I fix this?
                    logger.warning(
                        f"The image urls for {municipality} in {year} were not scraped"
                    )
                    pass

            # Step 4: Loop through all rows of the table to get target information
            # We do not include the last index because it will result in an error
            for result in sold_listings:

                sold_listings_dictionary["MLSNUM"].append(
                    result.find("td", {"class": "mlnum"}).a.get_text().strip()
                )
                address = result.find("td", {"class": "address"}).find_all("a")[-1]
                latlong = latlong_pattern.search(str(address))
                sold_listings_dictionary["LATITUDE"].append(latlong.group(1))
                sold_listings_dictionary["LONGITUDE"].append(latlong.group(2))

        return sold_listings_dictionary

    @staticmethod
    def kill_logger(logger_var, file_handler, console_handler):

        logger_var.removeHandler(file_handler)
        logger_var.removeHandler(console_handler)
        logging.shutdown()

    @staticmethod
    def last_day_of_month(month, year):

        target = calendar.month(year, month)
        # Isolate the last week of the month, then the last day of that week
        return target.split("\n")[-2].split(" ")[-1]

    def load_metadata(self, first_run=None):

        query = """
                SELECT * FROM gsmls_event_log_new
                ORDER BY id DESC
                LIMIT 1;
                """

        metadata = pd.read_sql_query(query, self.engine)
        last_row = metadata.shape[0] - 1

        if metadata.empty:
            pass

        else:

            if first_run is None:
                # Block is initiated on program start. Start date is the date of last run + 1 day
                self.start_date = metadata.loc[last_row, "date_produced"] + timedelta(
                    days=1
                )
            else:
                # Block is initiated if program restarts without completion.
                # Start date is the date of last scraped municipality
                self.start_date = metadata.loc[last_row, "start_date"]

            self.last_scraped_qtr = metadata.loc[last_row, "quarter"]
            self.last_scraped_year = metadata.loc[last_row, "year_"]
            self.last_scraped_county = metadata.loc[last_row, "county"]
            self.last_scraped_muni = metadata.loc[last_row, "municipality"]
            self.finished = metadata.loc[last_row, "finished"]
            self.last_scraped_property_type = metadata.loc[last_row, "property_type"]

            # All data from last run was scraped. Reset the value to scrape all new data
            if (
                self.last_scraped_county == 30
                and self.last_scraped_muni == "White Twp."
                and self.finished == "Yes"
            ):
                self.last_scraped_muni = None
                self.last_scraped_county = None

    def login(self, website, driver_var):
        """

        :param website:
        :param driver_var:
        :return:
        """

        if self.remote is False:
            username, _, pw = get_us_pw(website)
        else:
            username = os.getenv("GSMLS_USER")
            pw = os.getenv("GSMLS_PASSWORD")

        GSMLS.explicit_page_load("Login", driver_var)

        gsmls_id = driver_var.find_element(By.ID, "usernametxt")
        gsmls_id.click()
        gsmls_id.send_keys(username)
        password = driver_var.find_element(By.ID, "passwordtxt")
        password.click()
        password.send_keys(pw)
        login_button = driver_var.find_element(By.ID, "login-btn")
        login_button.click()
        time.sleep(1.5)  # Built-in latency
        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, "html.parser")

        # Check if there's a duplicate session running. If so, terminate it
        duplicate = soup.find(
            "input",
            {
                "class": "gs-btn-submit-sh gs-btn-submit-two tertiary-color ps-tertiary-color fs14 popup_button_0"
            },
        )
        if type(duplicate) == bs4.element.Tag:
            terminate_duplicate_session = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="alert_popup"]/div/div[2]/input[1]')
                )
            )
            terminate_duplicate_session.click()

        time.sleep(1)
        page_results = driver_var.page_source
        soup = BeautifulSoup(page_results, "html.parser")
        notice_msg = soup.find("div", {"id": "notice-box"})

        # Check if there's a GSMLS popup notice. If so, close the message
        if type(notice_msg) == bs4.element.Tag:
            try:
                ok_button = WebDriverWait(driver_var, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@value='OK']"))
                )
                ok_button.click()
            except TimeoutException:
                pass

    @staticmethod
    def no_results(driver_var):

        # page_source = driver_var.page_source
        # soup = BeautifulSoup(page_source, 'html.parser')
        # message_box = soup.find('div', {'id': 'message-box-container', 'class': 'active-container'})

        # if message_box:
        WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='popup_inner']"))
        )
        no_results_found = WebDriverWait(driver_var, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@value='OK']"))
        )
        no_results_found.click()
        # time.sleep(1)  # Built-in latency

    @staticmethod
    def page_criteria(timeframe, prop_type, driver_var):

        # Locate and click the Status tab
        x_path = '//*[@id="advance-search-fields"]/li[2]'
        status_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path))
        )
        status_tab.click()
        time.sleep(1)

        uncheck_all = driver_var.find_element(By.ID, "adv-uncheck-all")
        uncheck_all.click()  # Step 2: Uncheck unwanted statuses

        if timeframe == "historic":
            # Click the radio symbols which return historic data
            target_status = {
                "RES": ["SD", "WD", "XD"],
                "MUL": ["SD", "WD", "XD"],
                "LND": ["SD", "WD", "XD"],
                "RNT": ["RD", "WD", "XD"],
            }

        elif timeframe == "mixed":
            # Click the radio symbols which return historic data
            target_status = {
                "RES": ["S", "W", "X", "SD", "WD", "XD"],
                "MUL": ["S", "W", "X", "SD", "WD", "XD"],
                "LND": ["S", "W", "X", "SD", "WD", "XD"],
                "RNT": ["R", "W", "X", "RD", "WD", "XD"],
            }

        elif timeframe == "current":
            # Click the radio symbols which return historic data
            target_status = {
                "RES": ["S", "W", "X"],
                "MUL": ["S", "W", "X"],
                "LND": ["S", "W", "X"],
                "RNT": ["R", "W", "X"],
            }

        for target in target_status[prop_type]:
            status = driver_var.find_element(By.ID, target)
            status.click()  # Step 3: Check the sold status

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
        soup = BeautifulSoup(page_results, "html.parser")
        target = soup.find("li", {"class": "nav-header", "id": "2"})
        submenu_id = target.find("a", {"href": "#", "class": "has-submenu"})["id"]
        main_search = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, submenu_id))
        )
        main_search.click()

        # Click the Advanced Search sub-menu
        quicksearch_menu = target.find("li", {"id": f"2_{search_type}"})
        quicksearch_menu_id = quicksearch_menu.find(
            "a", {"href": "#", "class": "disabled has-submenu"}
        )["id"]
        extended_search_menu = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, quicksearch_menu_id))
        )
        extended_search_menu.click()

        # Click the XPY option which allows the access of RES, MUL and LND sales data
        xpy_search = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.ID, f"2_{search_type}_1"))
        )
        xpy_search.click()

    def publish_data_2kafka(self, xls_file_name: str, soldlistings: dict, **kwargs):

        topic_dict = {
            "RES": "res_properties",
            "MUL": "mul_properties",
            "LND": "lnd_properties",
            "RNT": "rnt_properties",
            "TAX": "tax_properties",
        }

        base_path = "C:\\Users\\Omar\\Desktop\\Selenium Temp Folder"
        kafka_data_prod = kwargs["data-producer"]

        if GSMLS.download_complete(xls_file_name):
            sold_df = pd.read_excel(
                os.path.join(base_path, xls_file_name + ".xls"), engine="xlrd"
            )
            sold_df.columns = sold_df.columns.str.upper()
            sold_df = GSMLS.return_target_columns(sold_df, kwargs["Property_Type"])

            # Merge the Latitude and Longitude data from the image df to the sold listings df
            if kwargs["Property_Type"] in ["RES", "MUL", "LND", "RNT"]:
                sold_df = sold_df.astype({"MLSNUM": "string"})

                if kwargs["Property_Type"] == "LND":
                    geo_data = pd.DataFrame(
                        {
                            "MLSNUM": soldlistings["MLSNUM"],
                            "LATITUDE": soldlistings["LATITUDE"],
                            "LONGITUDE": soldlistings["LONGITUDE"],
                        }
                    )
                else:
                    geo_data = pd.DataFrame(
                        {
                            "MLSNUM": soldlistings["MLSNUM"],
                            "LATITUDE": soldlistings["LATITUDE"],
                            "LONGITUDE": soldlistings["LONGITUDE"],
                            "IMAGES": soldlistings["IMAGES"],
                        }
                    )

                target_df = pd.merge(sold_df, geo_data, on="MLSNUM")
                target_df["MLS"] = "GSMLS"
                target_df["QTR"] = kwargs["Qtr"]
                target_df["CONDITION"] = "Unknown"
                target_df["PROP_CLASS"] = kwargs["Property_Type"]
                target_df["SCRAPED_DATE"] = datetime.today().date()

            elif kwargs["Property_Type"] == "TAX":
                target_df = sold_df

            # Send to Kafka
            final_df = target_df.to_json(orient="split", date_format="iso")

            try:
                result = kafka_data_prod.send(
                    topic_dict[kwargs["Property_Type"]],
                    key=xls_file_name,
                    value=final_df,
                )
                result_metadata = result.get(timeout=10)

            except KafkaTimeoutError as kte:
                kwargs["logger"].warning(
                    f"Kafka Producer Error: {xls_file_name} was not produced to {topic_dict[kwargs['Property_Type']]}"
                )
                kwargs["logger"].warning(f"{kte}")
                kwargs["logger"].info(f"Re-attempting to send {xls_file_name}")
                self.publish_data_2kafka(xls_file_name, soldlistings, **kwargs)
                GSMLS.sendfile2trash(xls_file_name)

            except MessageSizeTooLargeError:

                GSMLS.reduce_df_size(
                    kafka_data_prod,
                    target_df,
                    500,
                    topic_dict[kwargs["Property_Type"]],
                    xls_file_name,
                    kwargs["logger"],
                )
                GSMLS.sendfile2trash(xls_file_name)

            else:
                kwargs["logger"].info(
                    f'{xls_file_name} was produced to "{result_metadata.topic}" in "Partition {result_metadata.partition}" on "Offset {result_metadata.offset}"'
                )
                self.rows_counted[kwargs["Property_Type"]] += len(sold_df)
                self.download_log["Rows_Produced"][-1] = len(sold_df)
                self.download_log["Date_Produced"][-1] = str(datetime.now())
                GSMLS.sendfile2trash(xls_file_name)

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

        logger = kwargs["logger"]
        qtr = kwargs["Qtr"]
        date_range = kwargs["Dates"]
        # property_types = ['RES', 'MUL', 'LND', 'RNT', 'TAX']
        property_types = ["RES"]

        with tqdm(
            total=len(property_types), desc="Property Types", colour="magenta"
        ) as properties_bar:
            for type_ in property_types:

                if self.last_scraped_property_type is not None:
                    if type_ != self.last_scraped_property_type:
                        properties_bar.update(1)
                        time.sleep(0.2)
                        continue
                    else:
                        self.last_scraped_property_type = None
                        kwargs["Property_Type"] = type_

                # Click the property type in the dropdown menu
                GSMLS.click_property_type(type_, driver_var)

                # Set the page criteria
                # Make the timeframe an instance var that can dynamically change
                try:
                    GSMLS.page_criteria(self.timeframe, type_, driver_var)
                except selenium.common.exceptions.ElementNotInteractableException:
                    # The TAX property type doesn't have an uncheck all option or set page criteria
                    pass

                # Set the dates
                GSMLS.set_dates(date_range, type_, driver_var)
                # If this is rent the property styles
                if type_ == "RNT":
                    GSMLS.res_property_styles(driver_var)

                with tqdm(
                    total=len(self.municipalities.keys()),
                    desc="Counties",
                    colour="yellow",
                ) as counties_bar:
                    for county, municipality in self.municipalities.items():

                        if self.last_scraped_county is not None:
                            if int(county) != self.last_scraped_county:
                                counties_bar.update(1)
                                time.sleep(0.2)
                                continue
                            else:
                                self.last_scraped_county = None

                        GSMLS.click_target_tab("County", type_, driver_var)
                        GSMLS.set_county(2, county, driver_var)  # Set the county
                        GSMLS.click_target_tab("Town", type_, driver_var)

                        with tqdm(
                            total=len(municipality.keys()),
                            desc="Municipalities",
                            colour="green",
                            position=1,
                        ) as muni_bar:
                            for city_id, city_name in municipality.items():

                                if self.last_scraped_muni is not None:
                                    if city_name != self.last_scraped_muni:
                                        muni_bar.update(1)
                                        time.sleep(0.2)
                                        continue
                                    else:
                                        self.last_scraped_muni = None

                                GSMLS.set_city(2, city_id, driver_var)  # Set the city
                                GSMLS.explicit_page_load("Pre-Results", driver_var)
                                zero_results, too_many_results = GSMLS.show_results(
                                    driver_var
                                )

                                self.download_log["Year_"].append(kwargs["Year"])
                                self.download_log["Quarter"].append(qtr)
                                self.download_log["County"].append(county)
                                self.download_log["Municipality"].append(city_name)
                                self.download_log["Initiated"].append("Yes")
                                self.download_log["Finished"].append("No")
                                self.download_log["Rows_Produced"].append(0)
                                self.download_log["Date_Produced"].append(
                                    datetime.now().date()
                                )
                                self.download_log["Start_Date"].append(self.start_date)
                                self.download_log["Property_Type"].append(type_)

                                if zero_results is True:
                                    # No results found
                                    self.download_log["Results_Found"].append("No")
                                    self.download_log["Finished"][-1] = "Yes"
                                    GSMLS.no_results(driver_var)
                                    muni_bar.update(1)
                                    logger.info(
                                        f"There is no GSMLS sales data available for {city_name}"
                                    )
                                    GSMLS.click_target_tab("Town", type_, driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)
                                elif too_many_results is True:
                                    # Too many results were found, split the search dates
                                    self.download_log["Results_Found"].append("Yes")
                                    self.split_search_dates(
                                        kwargs["Year"],
                                        type_,
                                        city_name,
                                        county,
                                        driver_var,
                                        **kwargs,
                                    )
                                    self.download_log["Finished"][-1] = "Yes"
                                    GSMLS.set_dates(date_range, type_, driver_var)
                                    GSMLS.click_target_tab("Town", type_, driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)

                                else:
                                    # Results were found
                                    # Sales file will be requested and additional data will be added
                                    # and formatted before being produced to Apache Kafka
                                    self.download_log["Results_Found"].append("Yes")
                                    filename = self.download_sales_data(
                                        city_name,
                                        self.counties[county],
                                        qtr,
                                        kwargs["Year"],
                                        kwargs["Property_Type"],
                                        driver_var,
                                        kwargs["Main_Window"],
                                        logger,
                                    )
                                    GSMLS.explicit_page_load(
                                        "Results",
                                        driver_var,
                                        property_type=kwargs["Property_Type"],
                                    )

                                    if filename != "Server Error":
                                        additional_info = GSMLS.format_data_for_kafka(
                                            driver_var,
                                            kwargs["Year"],
                                            city_name,
                                            kwargs["Property_Type"],
                                            logger,
                                        )
                                        self.publish_data_2kafka(
                                            filename, additional_info, **kwargs
                                        )

                                    self.download_log["Finished"][-1] = "Yes"
                                    muni_bar.update(1)
                                    GSMLS.exit_results_page(driver_var)
                                    GSMLS.click_target_tab("Town", type_, driver_var)
                                    GSMLS.set_city(2, city_id, driver_var)

                        GSMLS.click_target_tab("County", type_, driver_var)
                        GSMLS.set_county(2, county, driver_var)  # Set the county
                        counties_bar.update(1)
                        self.save_metadata()
                        kwargs["data-producer"].flush()

                properties_bar.update(1)

    @staticmethod
    def reduce_df_size(producer, df_var, step: int, topic, file_name, logger):

        for idx, i in enumerate(range(0, len(df_var), step)):
            slice_df = df_var[i : i + step]

            prepared_df = slice_df.to_json(orient="split", date_format="iso")
            try:
                results = producer.send(topic, value=prepared_df)
                result_metadata = results.get(timeout=10)
                logger.info(
                    f'Block {idx} of {file_name} was produced to "{result_metadata.topic}" '
                    f'in "Partition {result_metadata.partition}" on "Offset {result_metadata.offset}"'
                )
            except MessageSizeTooLargeError:
                GSMLS.reduce_df_size(
                    producer, df_var, step // 5, topic, file_name, logger
                )

            except KafkaTimeoutError:
                logger.warning(
                    f"Property data for {file_name} has not been "
                    f"produced to {result_metadata.topic} in Kafka"
                )

    @staticmethod
    def res_property_styles(driver_var):
        """

        :param driver_var:
        :return:
        """

        # Locate and click the County tab
        x_path = '//*[@id="advance-search-fields"]/li[11]'
        property_tab = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.XPATH, x_path))
        )
        property_tab.click()

        for type_ in [
            "1Story",
            "1.5Story",
            "HalfDupl",
            "2Stories",
            "3+Story",
            "Apartmt",
            "Apartmnt",
            "Basement",
            "Condo",
            "Duplex",
            "FirstFlr",
            "FourPlex",
            "HighRise",
            "MultiFlr",
            "OneFloor",
            "TwnEndUn",
            "TwnIntUn",
            "Triplex",
        ]:
            WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, type_))
            ).click()

    @staticmethod
    def return_target_columns(df, ptypes: str):

        if ptypes == "RES":

            columns = [
                "MLSNUM",
                "STATUS_SHORT",
                "STREETNUMDISPLAY",
                "STREETNAME",
                "TOWN",
                "COUNTY",
                "ZIPCODE",
                "TOWNCODE",
                "COUNTYCODE",
                "BLOCKID",
                "LOTID",
                "TAXID",
                "DAYSONMARKET",
                "ORIGLISTPRICE",
                "LISTPRICE",
                "SALESPRICE",
                "SP/LP%",
                "LOANTERMS_SHORT",
                "ROOMS",
                "BEDS",
                "BATHSFULLTOTAL",
                "BATHSHALFTOTAL",
                "BATHSTOTAL",
                "SQFTAPPROX",
                "ACRES",
                "LOTSIZE",
                "ASSESSAMOUNTBLDG",
                "ASSESSAMOUNTLAND",
                "ASSESSTOTAL",
                "SUBPROPTYPE",
                "STYLEPRIMARY_SHORT",
                "STYLE_SHORT",
                "SUBDIVISION",
                "TAXAMOUNT",
                "TAXRATE",
                "TAXYEAR",
                "YEARBUILT",
                "LISTDATE",
                "PENDINGDATE",
                "ANTICCLOSEDDATE",
                "CLOSEDDATE",
                "EXPIREDATE",
                "WITHDRAWNDATE",
                "OWNERSHIP_SHORT",
                "EASEMENT_SHORT",
                "PARKNBRAVAIL",
                "DRIVEWAYDESC_SHORT",
                "GARAGECAP",
                "HEATSRC_SHORT",
                "HEATSYSTEM_SHORT",
                "COOLSYSTEM_SHORT",
                "WATER_SHORT",
                "UTILITIES_SHORT",
                "EXTERIOR_SHORT",
                "FIREPLACES",
                "FLOORS_SHORT",
                "POOL_SHORT",
                "ROOF_SHORT",
                "SEWER_SHORT",
                "SIDING_SHORT",
                "BASEMENT_SHORT",
                "BASEDESC_SHORT",
                "FLOODZONE",
                "ZONING",
                "APPFEE",
                "ASSOCFEE",
                "COMPBUY",
                "COMPSELL",
                "COMPTRANS",
                "LISTTYPE_SHORT",
                "OFFICELIST",
                "OFFICESELL",
                "OFFICESELLNAME",
                "AGENTSELLNAME",
                "OWNERNAME",
                "AGENTLIST",
                "AGENTSELL",
                "REMARKSAGENT",
                "REMARKSPUBLIC",
                "SHOWSPECIAL",
                "BUSRELATION_SHORT",
            ]

            return df[columns]

        elif ptypes == "MUL":

            columns = [
                "MLSNUM",
                "STATUS_SHORT",
                "STREETNUMDISPLAY",
                "STREETNAME",
                "TOWN",
                "COUNTY",
                "ZIPCODE",
                "TOWNCODE",
                "COUNTYCODE",
                "BLOCKID",
                "LOTID",
                "TAXID",
                "DAYSONMARKET",
                "ORIGLISTPRICE",
                "LISTPRICE",
                "SALESPRICE",
                "SP/LP%",
                "LOANTERMS_SHORT",
                "NUMUNITS",
                "ROOMS",
                "BEDS",
                "BATHSFULLTOTAL",
                "BATHSHALFTOTAL",
                "BATHSTOTAL",
                "SQFTBLDG",
                "ACRES",
                "LOTSIZE",
                "ASSESSAMOUNTBLDG",
                "ASSESSAMOUNTLAND",
                "ASSESSTOTAL",
                "UNITSTYLE_SHORT",
                "SUBDIVISION",
                "TAXAMOUNT",
                "TAXRATE",
                "TAXYEAR",
                "YEARBUILT",
                "INCOMEGROSSOPERATING",
                "EXPENSEOPERATING",
                "INCOMENETOPERATING",
                "EXPENSESINCLUDE_SHORT",
                "UNIT1BEDS",
                "UNIT1BATHS",
                "UNIT1ROOMS",
                "UNIT1OWNERTENANTPAYS_SHORT",
                "UNIT2BEDS",
                "UNIT2BATHS",
                "UNIT2ROOMS",
                "UNIT2OWNERTENANTPAYS_SHORT",
                "UNIT3BEDS",
                "UNIT3BATHS",
                "UNIT3ROOMS",
                "UNIT3OWNERTENANTPAYS_SHORT",
                "UNIT4BEDS",
                "UNIT4BATHS",
                "UNIT4ROOMS",
                "UNIT4OWNERTENANTPAYS_SHORT",
                "LISTDATE",
                "PENDINGDATE",
                "ANTICCLOSEDDATE",
                "CLOSEDDATE",
                "EXPIREDATE",
                "WITHDRAWNDATE",
                "EASEMENT_SHORT",
                "PARKNBRAVAIL",
                "DRIVEWAYDESC_SHORT",
                "GARAGECAP",
                "HEATSRC_SHORT",
                "HEATSYSTEM_SHORT",
                "COOLSYSTEM_SHORT",
                "WATER_SHORT",
                "UTILITIES_SHORT",
                "EXTERIOR_SHORT",
                "ROOF_SHORT",
                "SEWER_SHORT",
                "SIDING_SHORT",
                "BASEMENT_SHORT",
                "BASEDESC_SHORT",
                "FLOODZONE",
                "ZONING",
                "COMPBUY",
                "COMPSELL",
                "COMPTRANS",
                "LISTTYPE_SHORT",
                "OFFICELIST",
                "OFFICESELL",
                "OFFICESELLNAME",
                "AGENTSELLNAME",
                "OWNERNAME",
                "AGENTLIST",
                "AGENTSELL",
                "REMARKSAGENT",
                "REMARKSPUBLIC",
                "SHOWSPECIAL",
                "BUSRELATION_SHORT",
            ]

            return df[columns]

        elif ptypes == "LND":

            columns = [
                "MLSNUM",
                "STATUS_SHORT",
                "STREETNUMDISPLAY",
                "STREETNAME",
                "TOWN",
                "COUNTY",
                "ZIPCODE",
                "TOWNCODE",
                "COUNTYCODE",
                "BLOCKID",
                "LOTID",
                "TAXID",
                "DAYSONMARKET",
                "ORIGLISTPRICE",
                "LISTPRICE",
                "SALESPRICE",
                "SP/LP%",
                "LOANTERMS",
                "NUMLOTS",
                "ACRES",
                "LOTSIZE",
                "ASSESSAMOUNTBLDG",
                "ASSESSAMOUNTLAND",
                "ASSESSTOTAL",
                "SUBDIVISION",
                "TAXAMOUNT",
                "TAXRATE",
                "TAXYEAR",
                "LISTDATE",
                "PENDINGDATE",
                "ANTICCLOSEDDATE",
                "CLOSEDDATE",
                "EXPIREDATE",
                "WITHDRAWNDATE",
                "FLOODZONE",
                "ZONINGDESC_SHORT",
                "BUILDINGSINCLUDED_SHORT",
                "CURRENTUSE_SHORT",
                "DEVRESTRICT_SHORT",
                "DEVSTATUS_SHORT",
                "EASEMENT_SHORT",
                "IMPROVEMENTS_SHORT",
                "LOTDESC_SHORT",
                "PERCTEST_SHORT",
                "ROADFRONTDESC_SHORT",
                "ROADSURFACEDESC_SHORT",
                "SERVICES_SHORT",
                "SEWERINFO_SHORT",
                "SITEPARTICULARS_SHORT",
                "SOILTYPE_SHORT",
                "TOPOGRAPHY_SHORT",
                "WATERINFO_SHORT",
                "COMPBUY",
                "COMPSELL",
                "COMPTRANS",
                "LISTTYPE_SHORT",
                "OFFICELIST",
                "OFFICESELL",
                "OFFICESELLNAME",
                "AGENTSELLNAME",
                "OWNERNAME",
                "AGENTLIST",
                "AGENTSELL",
                "REMARKSAGENT",
                "REMARKSPUBLIC",
                "SHOWSPECIAL",
                "BUSRELATION_SHORT",
            ]

            return df[columns]

        elif ptypes == "RNT":

            columns = [
                "MLSNUM",
                "STATUS_SHORT",
                "STREETNUMDISPLAY",
                "STREETNAME",
                "TOWN",
                "COUNTY",
                "ZIPCODE",
                "TOWNCODE",
                "COUNTYCODE",
                "BLOCKID",
                "LOTID",
                "TAXID",
                "DAYSONMARKET",
                "RENTPRICEORIG",
                "LP",
                "RENTMONTHPERLSE",
                "RP/LP%",
                "RENTEDDATE",
                "LEASETERMS_SHORT",
                "ROOMS",
                "BEDS",
                "BATHSFULLTOTAL",
                "BATHSHALFTOTAL",
                "BATHSTOTAL",
                "SQFTAPPROX",
                "SUBDIVISION",
                "YEARBUILT",
                "PROPERTYTYPEPRIMARY_SHORT",
                "PROPSUBTYPERN",
                "LOCATION_SHORT",
                "PRERENTREQUIRE_SHORT",
                "OWNERPAYS_SHORT",
                "TENANTPAYS_SHORT",
                "TENANTUSEOF_SHORT",
                "RENTINCLUDES_SHORT",
                "RENTTERMS_SHORT",
                "LENGTHOFLEASE",
                "AVAILABLE_SHORT",
                "AMENITIES_SHORT",
                "APPLIANCES_SHORT",
                "LAUNDRYFAC",
                "FURNISHINFO_SHORT",
                "PETS_SHORT",
                "PARKNBRAVAIL",
                "DRIVEWAYDESC_SHORT",
                "BASEMENT_SHORT",
                "BASEDESC_SHORT",
                "GARAGECAP",
                "HEATSRC_SHORT",
                "HEATSYSTEM_SHORT",
                "COOLSYSTEM_SHORT",
                "WATER_SHORT",
                "UTILITIES_SHORT",
                "FLOORS_SHORT",
                "SEWER_SHORT",
                "TENLANDCOMM_SHORT",
                "REMARKSAGENT",
                "REMARKSPUBLIC",
                "SHOWSPECIAL",
            ]

            return df[columns]

        elif ptypes == "TAX":

            columns = [
                "AUTOROW",
                "CITYCODE",
                "BLOCKID",
                "BLOCKSUFFIX",
                "LOT",
                "LOTSUFFIX",
                "PARCEL_NO",
                "MCR",
                "MAP",
                "LOCNUM",
                "LOCDIR",
                "LOCSTREET",
                "LOCMODE",
                "LOCCITY",
                "LOCSTATE",
                "LOCZIP",
                "PROPERTYDESC",
                "PROPERTYUSECODE",
                "EQVALUE",
                "BANKCODE",
                "SALEDATE",
                "SALEPRICE",
                "TAXES",
                "TAXYR",
                "RATE",
                "RATIO",
                "RATIOYR",
                "TOTALASSESSMENT",
                "ASSESSMENT2",
                "ASSESSMENT1",
                "YEARBUILT",
                "BUILDINGDESC",
                "BUILDINGCLASSCODE",
                "ACRES",
                "ADDITIONALLOTS",
                "DEEDBOOK",
                "DEEDPAGE",
                "OWNER",
                "OWNERS",
                "MAILNUM",
                "MAILDIR",
                "MAILSTREET",
                "MAILMODE",
                "MAILCITY",
                "MAILSTATE",
                "MAILZIP",
                "PRIOROWNER",
                "PRIORSALEAMT",
                "PRIORSALEDATE",
                "PRIORDEEDBOOK",
                "PRIORDEEDPAGE",
                "DATEMODIFIED",
                "LCR",
            ]

            return df[columns]

    def save_metadata(self):

        metadata = pd.DataFrame(self.download_log)
        metadata.columns = metadata.columns.str.lower()
        metadata.to_sql(
            "gsmls_event_log_new", con=self.engine, if_exists="append", index=False
        )
        self.download_log = GSMLS.create_download_log()

    @staticmethod
    def scrape_image_links(dict_var, driver_var, link_var, prop_id):

        # Step 1: Find the respective media link and open it
        # print('Before media:', driver_var.window_handles)
        current_windows = driver_var.window_handles
        media_link = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    f'//*[@id="searchResult"]/main/div[1]/table/tbody/tr[{link_var}]/td[4]/a',
                )
            )
        )
        media_link.click()
        media_window = [
            window
            for window in driver_var.window_handles
            if window not in current_windows
        ][-1]

        # Step 2: Switch to new media links window
        driver_var.switch_to.window(media_window)
        WebDriverWait(driver_var, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        GSMLS.explicit_page_load(
            "Media Page", driver_var, prop_id=prop_id, window_id=media_window
        )

        # Step 3: Scrape the listing IDs
        soup = BeautifulSoup(driver_var.page_source, "html.parser")
        outercontainer = (
            soup.find("div", {"id": "outerContainer"})
            .find("form")
            .find("input", {"name": "sysIds"})
        )
        sys_ids = outercontainer["value"].split(",")

        # Step 3: Scrape the webpage and all associated high res image links starting from the first image link
        for listing_id in sys_ids[link_var - 1 :]:

            image_dictionary = {}

            GSMLS.explicit_page_load(
                "Media Page", driver_var, prop_id=listing_id, window_id=media_window
            )

            soup = BeautifulSoup(driver_var.page_source, "html.parser")
            images_list = soup.find_all("div", {"class": "imageReportContainer"})

            if len(images_list) > 0:
                raw_property_address = (
                    soup.find("div", {"class": "imagesReportTitle"})
                    .get_text(strip=True)
                    .split("•")[1]
                    .strip()
                )
                clean_address = GSMLS.clean_address(raw_property_address)
                for image_num, image in enumerate(images_list):
                    # The high res image is in the value attribute of the first input tag
                    image_dictionary[
                        f"{clean_address} - {image.get_text(strip=True)} - {image_num}"
                    ] = image.input["value"]

                dict_var["IMAGES"].append(image_dictionary)
            elif len(images_list) == 0:
                dict_var["IMAGES"].append("None")

            if listing_id != sys_ids[-1]:
                try:
                    # Step 4: Find 'NEXT' link to cycle through the list of property pictures
                    time.sleep(random.uniform(0.8, 1.7))
                    next_button = WebDriverWait(driver_var, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//a[normalize-space()='Next']")
                        )
                    )
                    next_button.click()
                except UnexpectedAlertPresentException:
                    # This alert is raised when the 'Next' button is clicked and there are no more properties left in the list
                    alert = Alert(driver_var)
                    alert.accept()
                    driver_var.close()
            else:
                driver_var.close()

    def scrape_municipalities(self, county_id, driver_var):

        GSMLS.set_county(1, county_id, driver_var)
        time.sleep(1)  # Latency period added in order to load and scrape city names
        self.find_cities(county_id, driver_var.page_source)
        GSMLS.set_county(1, county_id, driver_var)

    @staticmethod
    def search_listing(mls_number, driver_var, logger_var, mls_address=None):
        # Type in the MLS number or address. Create own function thats able to use both if necessary
        search_listing = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.ID, "qcksrchmlstxt"))
        )
        search_listing.click()
        search_listing.send_keys(mls_number)

        time.sleep(3.5)  # Built in latency to allow the table to populate
        page_results = driver_var.page_source

        return page_results

    @staticmethod
    def sendfile2trash(xls_file_name: str):

        send2trash.send2trash(
            os.path.join(
                "C:\\Users\\Omar\\Desktop\\Selenium Temp Folder", xls_file_name + ".xls"
            )
        )

    @staticmethod
    def set_city(search_type, city_id_var, driver_var):

        if search_type == 1:
            click_city = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, city_id_var))
            )
            click_city.click()
        else:
            # The ID variable should have "town" in front of it
            click_city = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "town" + str(city_id_var)))
            )
            click_city.click()

    @staticmethod
    def set_county(search_type, county_id_var, driver_var):

        if search_type == 1:
            click_county = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, county_id_var))
            )
            click_county.click()
        else:
            # The ID variable should have "county" in front of it
            click_county = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, "county" + str(county_id_var)))
            )
            click_county.click()

    @staticmethod
    def set_dates(date_range, type_, driver_var):

        ids_list = [
            ("CLOSEDDATEmin", "CLOSEDDATEmax"),
            ("EXPIREDATEmin", "EXPIREDATEmax"),
            ("WITHDRAWNDATEmin", "WITHDRAWNDATEmax"),
        ]
        rent_ids = [
            ("RENTEDDATEmin", "RENTEDDATEmax"),
            ("EXPIREDATEmin", "EXPIREDATEmax"),
            ("WITHDRAWNDATEmin", "WITHDRAWNDATEmax"),
        ]
        tax_ids = [("SALEDATEmin", "SALEDATEmax")]

        # XPath ID values which correspond to the closed, expired and withdrawn dates
        values = {
            "RES": [48, 66, 174],
            "MUL": [33, 47, 174],
            "LND": [31, 42, 102],
            "RNT": [142, 61, 172],
            "TAX": [81],
        }

        if type_ in ["RES", "MUL", "LND"]:
            target_list = ids_list
        elif type_ == "RNT":
            target_list = rent_ids
        else:
            target_list = tax_ids

        for value, daterangeids in zip(values[type_], target_list):
            # Locate and click the target tab
            x_path = f'//*[@id="advance-search-fields"]/li[{value}]'
            target_tab = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.XPATH, x_path))
            )
            target_tab.click()
            time.sleep(1)

            starting_close_date = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, daterangeids[0]))
            )
            starting_close_date.click()  # Step 5: Choose start date
            AC(driver_var).key_down(Keys.CONTROL).send_keys("A").key_up(
                Keys.CONTROL
            ).send_keys(date_range[0]).perform()
            ending_close_date = WebDriverWait(driver_var, 10).until(
                EC.presence_of_element_located((By.ID, daterangeids[1]))
            )
            ending_close_date.click()  # Step 6: Choose end date
            AC(driver_var).key_down(Keys.CONTROL).send_keys("A").key_up(
                Keys.CONTROL
            ).send_keys(date_range[1]).perform()

    @staticmethod
    def show_results(driver_var):

        show_results = WebDriverWait(driver_var, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "show"))
        )
        show_results.click()

        # Search for the message box div and check if the container is active
        try:
            WebDriverWait(driver_var, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # WebDriverWait(driver_var, 5).until(
            #     EC.presence_of_element_located((By.XPATH, "//div[@id='message-box-container']")))
            WebDriverWait(driver_var, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[@class='popup_inner']")
                )
            )
            WebDriverWait(driver_var, 5).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//h2[normalize-space()='Alert']")
                )
            )
        except TimeoutException:
            pass

        soup = BeautifulSoup(driver_var.page_source, "html.parser")
        # message_box_active = soup.find('div', {'id': 'message-box-container', 'class':'active-container'})
        message_box_active = soup.find(
            "div", {"id": "alert_popup", "class": "popup_outer active-container"}
        )

        if message_box_active is not None:
            box_message = message_box_active.find("div", {"class": "message"})
            message_text = box_message.get_text()

            if "500 records" in message_text:
                return False, True

            if "0 records" in message_text:
                return True, False

        else:
            return False, False

    @staticmethod
    def sign_out(driver_var):

        user = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="user"]/span[2]'))
        )
        user.click()
        sign_out_button = WebDriverWait(driver_var, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="logout"]'))
        )
        sign_out_button.click()

    def split_search_dates(self, year, type_, city_name, county, driver_var, **kwargs):
        """

        :param year:
        :param type_:
        :param city_name:
        :param county:
        :param driver_var:
        :param kwargs:
        :return:
        """

        # Too many results were found, exit the alert popup and continue with script
        GSMLS.too_many_results(driver_var)

        time_periods = {
            1: [f"01/01/{year}", f"03/31/{year}"],
            2: [f"04/01/{year}", f"06/30/{year}"],
            3: [f"07/01/{year}", f"09/30/{year}"],
            4: [f"10/01/{year}", f"12/31/{year}"],
        }

        for qtr, daterange in time_periods.items():
            # Quarterly timeframe
            GSMLS.set_dates([daterange[0], daterange[1]], type_, driver_var)

            zero_results, too_many_results = GSMLS.show_results(driver_var)

            if zero_results is False and too_many_results is False:
                filename = self.download_sales_data(
                    city_name,
                    self.counties[county],
                    qtr,
                    year,
                    kwargs["Property_Type"],
                    driver_var,
                    kwargs["Main_Window"],
                    kwargs["logger"],
                )
                additional_info = GSMLS.format_data_for_kafka(
                    driver_var,
                    year,
                    city_name,
                    kwargs["Property_Type"],
                    kwargs["logger"],
                )
                self.publish_data_2kafka(filename, additional_info, **kwargs)
                GSMLS.exit_results_page(driver_var)

            elif too_many_results is True:
                # Monthly timeframe
                GSMLS.too_many_results(driver_var)
                start_month = int(daterange[0][:2])

                for month in range(start_month, start_month + 3):
                    last_day = GSMLS.last_day_of_month(month, year)

                    month_str = GSMLS.string_month(month)
                    GSMLS.set_dates(
                        [f"{month_str}/01/{year}", f"{month_str}/{last_day}/{year}"],
                        type_,
                        driver_var,
                    )

                    zero_results, too_many_results = GSMLS.show_results(driver_var)

                    if zero_results is False and too_many_results is False:
                        new_qtr = f"{qtr}_{month_str}"
                        filename = self.download_sales_data(
                            city_name,
                            self.counties[county],
                            new_qtr,
                            year,
                            kwargs["Property_Type"],
                            driver_var,
                            kwargs["Main_Window"],
                            kwargs["logger"],
                        )
                        GSMLS.explicit_page_load(
                            "Results", driver_var, property_type=kwargs["Property_Type"]
                        )
                        additional_info = GSMLS.format_data_for_kafka(
                            driver_var,
                            year,
                            city_name,
                            kwargs["Property_Type"],
                            kwargs["logger"],
                        )
                        self.publish_data_2kafka(filename, additional_info, **kwargs)
                        GSMLS.exit_results_page(driver_var)

                    elif too_many_results is True:
                        # Weekly timeframe
                        GSMLS.too_many_results(driver_var)

                        for week, last_day_of_week in enumerate(
                            range(8, int(last_day) + 1, 7)
                        ):

                            # Dispite the function name, this just right pads single digits with '0'
                            if week == 0:
                                start_of_the_week_str = GSMLS.string_month(
                                    last_day_of_week - 7
                                )
                            else:
                                start_of_the_week_str = GSMLS.string_month(
                                    last_day_of_week - 6
                                )

                            last_day_of_week_str = GSMLS.string_month(last_day_of_week)
                            GSMLS.set_dates(
                                [
                                    f"{month_str}/{start_of_the_week_str}/{year}",
                                    f"{month_str}/{last_day_of_week_str}/{year}",
                                ],
                                type_,
                                driver_var,
                            )

                            zero_results, too_many_results = GSMLS.show_results(
                                driver_var
                            )

                            if zero_results is False and too_many_results is False:
                                new_qtr = f"{qtr}_{month_str}_{week}"
                                filename = self.download_sales_data(
                                    city_name,
                                    self.counties[county],
                                    new_qtr,
                                    year,
                                    kwargs["Property_Type"],
                                    driver_var,
                                    kwargs["Main_Window"],
                                    kwargs["logger"],
                                )
                                GSMLS.explicit_page_load(
                                    "Results",
                                    driver_var,
                                    property_type=kwargs["Property_Type"],
                                )
                                additional_info = GSMLS.format_data_for_kafka(
                                    driver_var,
                                    year,
                                    city_name,
                                    kwargs["Property_Type"],
                                    kwargs["logger"],
                                )
                                self.publish_data_2kafka(
                                    filename, additional_info, **kwargs
                                )
                                GSMLS.exit_results_page(driver_var)

                            elif too_many_results is True:

                                GSMLS.too_many_results(driver_var, giveup="Yes")
                                new_qtr = f"{qtr}_{month_str}_{week}"
                                filename = self.download_sales_data(
                                    city_name,
                                    self.counties[county],
                                    new_qtr,
                                    year,
                                    kwargs["Property_Type"],
                                    driver_var,
                                    kwargs["Main_Window"],
                                    kwargs["logger"],
                                )
                                GSMLS.explicit_page_load(
                                    "Results",
                                    driver_var,
                                    property_type=kwargs["Property_Type"],
                                )
                                additional_info = GSMLS.format_data_for_kafka(
                                    driver_var,
                                    year,
                                    city_name,
                                    kwargs["Property_Type"],
                                    kwargs["logger"],
                                )
                                self.publish_data_2kafka(
                                    filename, additional_info, **kwargs
                                )
                                GSMLS.exit_results_page(driver_var)

    @staticmethod
    def string_month(value):

        string_month = str(value)

        if len(string_month) == 1:
            return "0" + string_month

        else:
            return value

    @staticmethod
    def too_many_results(driver_var, giveup=None):

        if giveup is None:
            no_button = WebDriverWait(driver_var, 30).until(
                EC.presence_of_element_located((By.XPATH, "//input[@value='No']"))
            )
            no_button.click()

        elif giveup == "Yes":
            yes_button = WebDriverWait(driver_var, 30).until(
                EC.presence_of_element_located((By.XPATH, "//input[@value='Yes']"))
            )
            yes_button.click()

    def login_load_main_page(self, driver_var, **kwargs):
        self.login("GSMLS", driver_var)
        GSMLS.explicit_page_load("Garden State MLS", driver_var)
        kwargs["Main_Window"] = driver_var.current_window_handle

    def scrape_state_data(self, driver_var):

        if self.municipalities == {}:
            page_results = driver_var.page_source
            GSMLS.page_search(2, page_results, driver_var)
            time.sleep(2)  # Build-in latency to let the page load

            # Scrape all the county and municipality targets
            self.create_state_dictionary(driver_var)

        page_results = driver_var.page_source
        GSMLS.page_search(1, page_results, driver_var)
        GSMLS.explicit_page_load("Advanced Search", driver_var)

    def value_generator(self, key_name, update_bar, values=None):

        instance_key_values = {
            'year': self.last_scraped_year,
            'property_type': self.last_scraped_property_type,
            'county': self.last_scraped_county,
            'municipality': self.last_scraped_muni
        }
        metadata_value = instance_key_values[key_name]

        if key_name == 'county':
            values = self.municipalities

        if key_name in ['county', 'municipalities']:

            for key, value_ in values.items():
                if metadata_value is not None:
                    if key_name == 'municipality':
                        target = value_
                    else:
                        target = key
                    if target != metadata_value:
                        update_bar.update(1)
                        time.sleep(0.2)
                        continue
                    else:
                        instance_key_values[key_name] = None
                        break

                yield key, value_

        else:

            for key, in values:
                if metadata_value is not None:
                    if key != metadata_value:
                        update_bar.update(1)
                        time.sleep(0.2)
                        continue
                    else:
                        instance_key_values[key_name] = None
                        break

                yield key

    @logger_decorator
    def main(self, driver_var, **kwargs):

        # Remove the logger decorator and just accept **kwargs
        logger = kwargs["logger"]
        f_handler = kwargs["f_handler"]
        c_handler = kwargs["c_handler"]
        if self.remote is not True:
            kwargs["data-producer"] = create_kafka_producer(
                "data_producer", logger=logger, remote=False
            )

        try:
            # Step 1: Login to the GSMLS
            self.login_load_main_page(driver_var, **kwargs)

            # Step 2: Choose the property search type
            page_results = driver_var.page_source
            if self.municipalities == {}:
                GSMLS.page_search(2, page_results, driver_var)
                time.sleep(2)  # Build-in latency to let the page load

                # Step 3: Scrape all the county and municipality targets
                self.create_state_dictionary(driver_var)
                page_results = driver_var.page_source

            GSMLS.page_search(1, page_results, driver_var)
            GSMLS.explicit_page_load("Advanced Search", driver_var)

            # Step 4: Create the time periods for which to search for data
            years = range(1995, datetime.now().year + 1)
            with tqdm(total=len(years), desc="Years", colour="red") as year_bar:
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

                    time_periods = self.assign_timeframe(year)

                    with tqdm(
                        total=len(time_periods), desc="Qtr", colour="blue", position=1
                    ) as quarters_bar:
                        for qtr, date_range in time_periods.items():

                            kwargs["Qtr"] = qtr
                            kwargs["Dates"] = date_range
                            kwargs["Year"] = year

                            self.quarterly_sales_res(driver_var, **kwargs)
                            quarters_bar.update(1)

                    year_bar.update(1)

            # Step 5: Sign out
            GSMLS.sign_out(driver_var)

        except (TimeoutException, TimeoutError, AttributeError):
            exc = sys.exception()
            logger.warning(f"{repr(traceback.format_exception(exc))}")
            logger.info(
                "Selenium Webdriver Timeout has been experienced. Restarting program..."
            )

        except AssertionError as AE:
            logger.warning(f"{AE}")
            GSMLS.kill_logger(logger, f_handler, c_handler)
            self.save_metadata()
            raise AssertionError

        except KeyboardInterrupt:
            # Press the stop button once in order for data to save
            logger.info("User has ended the program")
            GSMLS.kill_logger(logger, f_handler, c_handler)
            raise KeyboardInterrupt

        except BaseException:
            exc = sys.exception()
            logger.warning(f"{repr(traceback.format_exception(exc))}")
            logger.info("Unknown except has occurred:")

        finally:
            driver_var.quit()
            GSMLS.kill_logger(logger, f_handler, c_handler)
            self.save_metadata()

    def airflow_gsmls_producer(self, **kwargs):

        website = "https://mls.gsmls.com/member/"
        quit_program = False

        while quit_program:

            driver = create_selenium_webdriver()

            try:
                driver.maximize_window()
                driver.get(website)
                self.main(driver, **kwargs)

            except AssertionError:
                break

            except KeyboardInterrupt:
                self.save_metadata()
                break

            except NoBrokersAvailable:
                break

            else:
                self.load_metadata()
