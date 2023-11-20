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
from selenium.common.exceptions import InvalidArgumentException
from selenium.common.exceptions import NoSuchAttributeException
from selenium.common.exceptions import NoSuchDriverException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException


class Foreclosures:

    foreclosures1 = 'https://salesweb.civilview.com/'
    foreclosures2 = 'https://www.foreclosurelistings.com/list/NJ/'

    def __init__(self, county=None):
        if county is None:
            self._county = None
        else:
            self._county = county

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

    @staticmethod
    def find_counties(page_source):

        # Find the counties on the NJ Tax Assessment page
        value_pattern = re.compile(r'/Sales/SalesSearch\?countyId=(\d{1,4}?)')
        soup = BeautifulSoup(page_source, 'html.parser')
        # target = soup.find('select', {"name": "select_cc"})
        target_contents = soup.find_all('a', {'href': value_pattern})
        counties = {}

        for i in target_contents:
            if i.get_text().split(',')[1].strip() == 'NJ':  # Strips the text of the link to find if the county is located in NJ
                counties[int(value_pattern.search(i).group(1))] = i.get_text()

        return counties

    def foreclosure_listings(self, driver_var, county=None, **kwargs):
        """Scrape the second foreclosure website for listings"""
        pass

    def foreclosure_db_check(self):
        """Function which will check the current SQL DB against the updated website. If the information isn't
        the same as what's found on the website at the time of the check, the db will update the information
        """
        pass

    @staticmethod
    @logger_decorator
    def salesweb_foreclosures(driver_var, county=None, **kwargs):
        """Function which will scrape the salesweb.civilview website for all foreclosure
        listings in the NJ county supplied as an arg, or scrape for all NJ counties if no
        county arg is given"""

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        foreclosure_dict = {'County': [],
                            'Sheriff #': [],
                            'Court Case #': [],
                            'Sales Date': [],
                            'Plaintiff': [],
                            'Defendant': [],
                            'Address': [],
                            'Priors': [],
                            'Description': [],
                            'Approx. Upset*': [],
                            'Attorney': [],
                            'Attorney Phone': [],
                            'Status': []}
        url = Foreclosures.foreclosures1

        driver_var.get(url)

        try:
            WebDriverWait(driver_var, 5).until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div/div/h4")))  # Waits for the presence of the XPath which asks the user to click a link below

            results = driver_var.page_source
            counties = Foreclosures.find_counties(results)

            if county is None:  # Loop through the counties dictionary to scrape all foreclosure listings
                for value in counties.values():
                    county_option = driver_var.find_element(By.LINK_TEXT, value)
                    county_option.click()
                    WebDriverWait(driver_var, 5).until(EC.presence_of_element_located(
                        (By.XPATH,
                         "/html/body/div/div[1]/form/div/div[1]/div[1]/div[1]/input")))  # Waits for the presence of the XPath which is the Search bar on the top of the page
                    results1 = driver_var.page_source
                    Foreclosures.salesweb_scrape(results1, driver_var, foreclosure_dict, logger, value.split(',')[0])
            elif county is not None:
                for value in counties.values():
                    if (county.title() + ' County, NJ') != value:
                        continue
                    else:
                        county_option = driver_var.find_element(By.LINK_TEXT, value)
                        county_option.click()
                        WebDriverWait(driver_var, 5).until(EC.presence_of_element_located(
                            (By.XPATH,
                             "/html/body/div/div[1]/form/div/div[1]/div[1]/div[1]/input")))  # Waits for the presence of the XPath which is the Search bar on the top of the page
                        results1 = driver_var.page_source
                        Foreclosures.salesweb_scrape(results1, driver_var, foreclosure_dict, logger, value.split(',')[0])

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
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

        return foreclosure_dict

    @staticmethod
    def salesweb_scrape(page_source, driver_var, dict_for_foreclosure, logger, county=None):

        details_pattern = re.compile(r'/Sales/SalesDetails\?PropertyId=(\d{8,13}?)')
        date_pattern = re.compile(r'\d{1,2}?\\\d{1,2}?\\\d{4}')
        all_contents_pattern = re.compile(r'.*')

        soup = BeautifulSoup(page_source, 'html.parser')
        target_contents = soup.find_all('tr')
        details = []

        for i in target_contents:
            target_contents1 = i.find_all('td')
            for k in target_contents1:
                if date_pattern.search(k.get_text()) is None:
                    continue
                else:
                    date_found = date_pattern.search(k.get_text()).group()
                    if datetime.strptime(date_found, "%m\%d\%Y").date() <= datetime.today():
                        continue
                    else:
                        detail_link = i.find('a', {'href': details_pattern})
                        details.append(detail_link)
        try:
            for j in details:
                link = driver_var.find_find_element(By.CSS_SELECTOR, "[href='https://salesweb.civilview.com" + j + "]")
                link.click()
                results2 = driver_var.page_source
                soup1 = BeautifulSoup(results2, 'html.parser')
                target_contents2 = soup1.find_all('tbody')
                status = {}

                for idx, m in enumerate(target_contents2):
                    if idx == 0:
                        tbody_content = m.find_all('tr')
                        for n in tbody_content:
                            for child in n.children:
                                if child.get_text() == 'Sheriff #':
                                    dict_for_foreclosure['Sheriff #'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Court Case #':
                                    dict_for_foreclosure['Court Case #'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Sales Date':
                                    dict_for_foreclosure['Sales Date'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Plaintiff':
                                    dict_for_foreclosure['Plaintiff'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Defendant':
                                    dict_for_foreclosure['Defendant'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Address':
                                    dict_for_foreclosure['Address'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Priors':
                                    dict_for_foreclosure['Priors'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Description':
                                    dict_for_foreclosure['Description'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Attorney':
                                    dict_for_foreclosure['Attorney'].append(all_contents_pattern.search(child.next_sibling))
                                elif child.get_text() == 'Approx. Upset*':
                                    dict_for_foreclosure['Approx. Upset*'].append(float(''.join([(all_contents_pattern.search(
                                        child.next_sibling)).lstrip('$').split(',')])))
                                elif child.get_text() == 'Attorney Phone':
                                    dict_for_foreclosure['Attorney Phone'].append(all_contents_pattern.search(child.next_sibling))
                                else:
                                    continue
                    elif idx == 1:
                        tbody_content = m.find_all('tr')
                        for n in tbody_content:
                            for child in n.children:
                                status[all_contents_pattern.search(child).group()] = all_contents_pattern.search(
                                    child.next_sibling).group()

                dict_for_foreclosure['Status'].append(status)
                dict_for_foreclosure['County'] = county

                for key in dict_for_foreclosure.keys():
                    try:

                        assert len(dict_for_foreclosure[key]) == len(dict_for_foreclosure['County']), f'{key} is missing a value'

                    except AssertionError as AE:
                        logger.exception(f'{AE}')
                        dict_for_foreclosure[key].append('N/A')

                driver_var.back()

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

        except Exception as e:
            print(e)


if __name__ == '__main__':

    pass

