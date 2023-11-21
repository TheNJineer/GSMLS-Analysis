
import GSMLS
import MachineLearning
from NJTaxAssessment_v2 import NJTaxAssessment
import Foreclosures
from math import radians, sin, asin, sqrt


class DealAnalyzer(GSMLS):

    def __init__(self, address, offer_price= None, br= None, bath= None, sq_ft= None, home_type= None):
        # What information do I need to initialize an instance of this class?
        # Use RehabValuator.com as your goalpost. Very good website
        super.__init__(self)
        self._address = address
        self._offer_price = offer_price
        self._bathrooms = bath
        self._bedrooms = br
        self._sq_ft = sq_ft
        self._home_type = home_type
        self._latitude = 0.0
        self._longitude = 0.0

    @staticmethod
    def city_stats(city):
        # Produce the most recent city sales stats from the excel database
        pass

    @classmethod
    def target_property(cls, address, offer_price, br, bath, sq_ft, home_type):
        # Alt constructor for a prospective rehab property
        # Make sure to fortify the target property with the latitude and longitude
        pass

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
        :param property_address:
        :param br:
        :param bth:
        :param sq_ft:
        :param home_type:
        :return:
        """
        pass

    def haversine(self, latitude, longitude,  comp_db):
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