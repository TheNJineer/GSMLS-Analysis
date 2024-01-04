
from GSMLS import GSMLS
from MachineLearning import MachineLearning
from NJTaxAssessment_v2 import NJTaxAssessment
from Foreclosures import Foreclosures
from math import radians, sin, asin, sqrt
import pandas as pd
import logging
from datetime import datetime


class DealAnalyzer(GSMLS):

    def __init__(self, address, offer_price= None, br= None, bath= None, sq_ft= None, home_type= None):
        # What information do I need to initialize an instance of this class?
        # Use RehabValuator.com as your goalpost. Very good website
        GSMLS.__init__(self)
        self._address = address
        self._offer_price = offer_price
        self._bathrooms = bath
        self._bedrooms = br
        self._sq_ft = sq_ft
        self._home_type = home_type
        self._latitude = 0.0
        self._longitude = 0.0

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
    def city_stats(city):
        # Produce the most recent city sales stats from the excel database
        pass

    def deal_analyzer_res(self, property_address, br=None, bth=None, sq_ft=None, home_type=None):
        """
        Method which determines the profitability of a deal.
        Accepts a str or list object. If string is provided, it will run the analysis for one property. If a string
        is provided, it will run the analysis for multiple properties then evaluate them against once another
        and suggest which one should be pursued

        It populates all the general information about the house,
        purchase information, potential levels of rehab needed, uses the comp method to run comps and input into the
        comp section. Also uses the predicted home price from the SVM and Linear Regression Model
        as well as the DecisionTree Model to gauge profitability of the project.
        Class methods to use:
        - comps
        - renovation estimates
        - population
        - area demographics
        - SVM
        - LinearRegression
        :param property_address:
        :param br:
        :param bth:
        :param sq_ft:
        :param home_type:
        :return:
        """
        pass

    def deal_analyzer_mul(self, property_address, br=None, bth=None, sq_ft=None, home_type=None):
        """
        Method which determines the profitability of a multi-family deal.
        Accepts a str or list object. If string is provided, it will run the analysis for one property. If a string
        is provided, it will run the analysis for multiple properties then evaluate them against once another
        and suggest which one should be pursued

        It populates all the general information about the house,
        purchase information, potential levels of rehab needed, uses the comp method to run comps and input into the
        comp section. Also uses the predicted home price from the SVM and Linear Regression Model
        as well as the DecisionTree Model to gauge profitability of the project.
        Class methods to use:
        - comps
        - renovation estimates
        - population
        - area demographics
        - SVM
        - LinearRegression
        :param property_address:
        :param br:
        :param bth:
        :param sq_ft:
        :param home_type:
        :return:
        """
        pass

    def entitlement(self, city):
        """
        Create a method which addresses the applicable zoning regulations, municipal codes, and
        neighborhood council/community groups requirements — resulting in project
        approval from a Planning Commission or City Council.
        :param city:
        :return:
        """
        pass

    def email_deals(self):
        """
        Method which logs into my email and pulls the MLS IDs from the mail then running the deal_analyzer.
        Returns a df of properties which meet my criteria and a
        df for which ones that don't
        :return:
        """
        pass

    def haversine(self, latitude, longitude, comp_db):
        """

        :param latitude:
        :param longitude:
        :param comp_db:
        :return:
        """
        # Creates a Pandas Series which will be attached to the comps db.
        # Calculates the distance between a subject property and comps
        # Comp_db should be a Pandas db of recently pulled comps from GSMLS
        # Lat2 and Long2 should be from the comp_db
        r_earth_avg = 3956.6
        x = (sin((radians(comp_db['Latitude']) - radians(latitude)) / 2)) ** 2
        y = (sin((radians(comp_db['Latitude']) + radians(latitude)) / 2)) ** 2
        z = (sin((radians(comp_db['Longitude']) - radians(longitude)) / 2)) ** 2
        h = x + (1 - x - y) * z
        comp_db['Distance'] = round(2 * r_earth_avg * asin(sqrt(h)), 3)

        return comp_db

    @staticmethod
    def pivot_table_by_cy():
        """
        Creates a county level pivot table separated by year using the NJ Realtor 10k data stored in PostgreSQL
        You can index a pivot with this syntax: df_pivot.loc[('County_Name County', 'YYYY'), :]
        :return:
        """
        sql_table_object = ''
        df_pivot = pd.pivot_table(sql_table_object, values=['Closed Sales', 'Days on Markets', 'Median Sales Prices',
                                                            'Percent of Listing Price Received', 'Months of Supply'],
                                  index=['County', 'Year'], columns=['Quarter'], fill_value=0,
                                  aggfunc={'Closed Sales': "sum",
                                           'Days on Markets': "mean",
                                           'Median Sales Prices': "mean",
                                           'Percent of Listing Price Received': "mean",
                                           'Months of Supply': "mean"})

        return df_pivot

    @staticmethod
    def pivot_table_cy_exploded():
        """
        Creates a county level pivot table separated by year and by municipality using the NJ Realtor 10k data stored in PostgreSQL
        You can index a pivot with this syntax: df_pivot.loc[('County_Name County', 'YYYY'), :]
        or use MultiIndex/Advanced Index slicing to get to a more granular level
        :return:
        """
        sql_table_object = ''
        df_pivot = pd.pivot_table(sql_table_object, values=['Closed Sales', 'Days on Markets', 'Median Sales Prices',
                                                            'Percent of Listing Price Received', 'Months of Supply'],
                                  index=['County', 'Year', 'City'], columns=['Quarter'], fill_value=0,
                                  aggfunc={'Closed Sales': "sum",
                                           'Days on Markets': "mean",
                                           'Median Sales Prices': "mean",
                                           'Percent of Listing Price Received': "mean",
                                           'Months of Supply': "mean"})

        return df_pivot

    def renovation_estimates(self, sq_ft, bd, bath):
        """

        :param sq_ft:
        :param bd:
        :param bath:
        :return:
        """
        # Do a renovation analysis to provide a range of renovation estimates
        # based on sq_ft and bedroom sizes and do a more detailed itemized analysis
        pass

    @classmethod
    def target_property(cls, address, offer_price, br, bath, sq_ft, home_type):
        # Alt constructor for a prospective rehab property
        # Make sure to fortify the target property with the latitude and longitude
        pass