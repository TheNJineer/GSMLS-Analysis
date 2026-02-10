import requests
import json
import math
import os
import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm
from dotenv import load_dotenv
from gsmls.utility_func import create_sql_engine, create_postgres_connection, get_filepath


class GSMLSCleaning:

    def __init__(self, table_name):
        self.table_name = table_name
        self.ncjar_connection = create_sql_engine('nj_realtor_data', True)
        self.tax_connection = create_sql_engine('nj_tax_assessor', True)
        self.gsmls_connection = create_sql_engine('gsmls', True)
        self.county_codes = self.get_county_codes()

    @staticmethod
    def convert_bool_columns(df: pd.DataFrame):

        old_boolean_columns = {i: 'int64' for i in df.columns if df[i].dtype == 'bool'}
        df = df.astype(old_boolean_columns)

        print(' ==== CONVERTING BOOLEAN COLUMNS IS COMPLETE ==== ')

        return df

    @staticmethod
    def create_aggregate_data(df: pd.DataFrame):

        print(' ==== CREATING AGGREGATE DATA ==== ')

        agg_data = df.groupby(['YEAR', 'COUNTY', 'TOWN']).agg(
            transaction_count=pd.NamedAgg(column='STATUS_SHORT', aggfunc='count'),
            list_price_mean=pd.NamedAgg(column='LISTPRICE', aggfunc='mean'),
            list_price_std=pd.NamedAgg(column='LISTPRICE', aggfunc='std'),
            sales_price_mean=pd.NamedAgg(column='SALESPRICE', aggfunc='mean'),
            sales_price_std=pd.NamedAgg(column='SALESPRICE', aggfunc='std'),
            days_on_market_mean=pd.NamedAgg(column='DAYSONMARKET', aggfunc='mean'),
            days_on_market_std=pd.NamedAgg(column='DAYSONMARKET', aggfunc='std'),
            sq_ft_mean=pd.NamedAgg(column='SQFTAPPROX', aggfunc='mean'),
            sq_ft_std=pd.NamedAgg(column='SQFTAPPROX', aggfunc='std'),
            year_built_mean=pd.NamedAgg(column='YEARBUILT', aggfunc='mean'),
            year_built_std=pd.NamedAgg(column='YEARBUILT', aggfunc='std'),
            age_of_property_mean=pd.NamedAgg(column='AGE_OF_PROPERTY', aggfunc='mean'),
            age_of_property_std=pd.NamedAgg(column='AGE_OF_PROPERTY', aggfunc='std'),
            lot_size_mean=pd.NamedAgg(column='LOTSIZE (SQFT)', aggfunc='mean'),
            lot_size_std=pd.NamedAgg(column='LOTSIZE (SQFT)', aggfunc='std'),
            rooms_mean=pd.NamedAgg(column='ROOMS', aggfunc='mean'),
            rooms_std=pd.NamedAgg(column='ROOMS', aggfunc='std'),
            beds_mean=pd.NamedAgg(column='BEDS', aggfunc='mean'),
            beds_std=pd.NamedAgg(column='BEDS', aggfunc='std'),
            baths_mean=pd.NamedAgg(column='BATHSTOTAL', aggfunc='mean'),
            baths_std=pd.NamedAgg(column='BATHSTOTAL', aggfunc='std'),
            assessment_mean=pd.NamedAgg(column='ASSESSTOTAL', aggfunc='mean'),
            assessment_std=pd.NamedAgg(column='ASSESSTOTAL', aggfunc='std'),
            tax_mean=pd.NamedAgg(column='TAXAMOUNT', aggfunc='mean'),
            tax_std=pd.NamedAgg(column='TAXAMOUNT', aggfunc='std'),
            garages_mean=pd.NamedAgg(column='GARAGECAP', aggfunc='mean'),
            garages_std=pd.NamedAgg(column='GARAGECAP', aggfunc='std'),
            app_fee_mean=pd.NamedAgg(column='APPFEE', aggfunc='mean'),
            app_fee_std=pd.NamedAgg(column='APPFEE', aggfunc='std'),
            assoc_fee_mean=pd.NamedAgg(column='ASSOCFEE', aggfunc='mean'),
            assoc_fee_std=pd.NamedAgg(column='ASSOCFEE', aggfunc='std'),
            fireplace_mean=pd.NamedAgg(column='FIREPLACES', aggfunc='mean'),
            fireplace_std=pd.NamedAgg(column='FIREPLACES', aggfunc='std'),
            assess_to_lp_ratio_mean=pd.NamedAgg(column='assess_to_lp_ratio', aggfunc='mean'),
            assess_to_lp_ratio_std=pd.NamedAgg(column='assess_to_lp_ratio', aggfunc='std'),
        )

        agg_data.reset_index(inplace=True)

        return agg_data

    def create_nj_county_code(self, df: pd.DataFrame):
        """Use this function to create a new column with the nj town codes"""

        print(' ==== MERGING COUNTY CODES TO RESIDENTIAL DATA ==== ')

        county_df = self.county_codes.copy()
        county_df = county_df.rename(columns={'county': 'COUNTY', 'county_code': 'NJ_COUNTY_CODE'})
        county_df['COUNTY'] = county_df['COUNTY'].str.replace(' County', '')
        county_df = county_df[['COUNTY', 'NJ_COUNTY_CODE']]
        county_df = county_df.astype({'COUNTY': 'string', 'NJ_COUNTY_CODE': 'string'})
        df.loc[:, 'COUNTY'] = df.loc[:, 'COUNTY'].astype('string')

        print(' ==== MERGE COMPLETE ==== ')

        return df.merge(county_df, on="COUNTY")

    @staticmethod
    def create_nj_town_code(df: pd.DataFrame):
        """Use this function to create a new column with the nj town codes"""

        town_values = df['TOWNCODE'].astype('string')
        town_values = town_values.str.slice(start=-2)
        new_town_codes = df['NJ_COUNTY_CODE'].str.cat(town_values)
        df.loc[:, 'NJ_TOWNCODE'] = new_town_codes
        df.loc[:, 'NJ_TOWNCODE'] = df.loc[:, 'NJ_TOWNCODE'].astype('string')

        df.pop('NJ_COUNTY_CODE')
        df.insert(7, 'NJ_TOWNCODE', df.pop('NJ_TOWNCODE'))

        print(' ==== CREATION OF NEW NJ TOWN CODES HAS BEEN COMPLETED')

        return df

    @staticmethod
    def create_zscore_cols(test_data: pd.DataFrame, agg_data: pd.DataFrame):

        # Enrich the test data with local statistics

        for year in test_data['YEAR'].unique():
            for county in test_data['COUNTY'].unique():

                # Isolate the data for that year and county
                target_mask = test_data[(test_data['YEAR'] == year) & (test_data['COUNTY'] == county)]

                for town in target_mask['TOWN'].unique():
                    town_mask = test_data[
                        (test_data['YEAR'] == year) & (test_data['COUNTY'] == county) & (test_data['TOWN'] == town)]
                    agg_mask = agg_data[
                        (agg_data['YEAR'] == year) & (agg_data['COUNTY'] == county) & (agg_data['TOWN'] == town)]

                    if agg_mask.empty:
                        continue

                    test_data.loc[town_mask.index, 'list_price_zscore'] = (town_mask['LISTPRICE'] -
                                                                           agg_mask['list_price_mean'].values[0]) / \
                                                                          agg_mask['list_price_std'].values[0]
                    test_data.loc[town_mask.index, 'list_to_sales_price_zscore'] = (town_mask['LISTPRICE'] -
                                                                                    agg_mask['sales_price_mean'].values[
                                                                                        0]) / \
                                                                                   agg_mask['sales_price_std'].values[0]
                    test_data.loc[town_mask.index, 'dom_zscore'] = (town_mask['DAYSONMARKET'] -
                                                                    agg_mask['days_on_market_mean'].values[0]) / \
                                                                   agg_mask['days_on_market_std'].values[0]
                    test_data.loc[town_mask.index, 'sqft_zscore'] = (town_mask['SQFTAPPROX'] -
                                                                     agg_mask['sq_ft_mean'].values[0]) / \
                                                                    agg_mask['sq_ft_std'].values[0]
                    test_data.loc[town_mask.index, 'year_built_zscore'] = (town_mask['YEARBUILT'] -
                                                                           agg_mask['year_built_mean'].values[0]) / \
                                                                          agg_mask['year_built_std'].values[0]
                    test_data.loc[town_mask.index, 'age_of_property_zscore'] = (town_mask['AGE_OF_PROPERTY'] -
                                                                                agg_mask['age_of_property_mean'].values[
                                                                                    0]) / \
                                                                               agg_mask['age_of_property_std'].values[0]
                    test_data.loc[town_mask.index, 'lot_size_zscore'] = (town_mask['LOTSIZE (SQFT)'] -
                                                                         agg_mask['lot_size_mean'].values[0]) / \
                                                                        agg_mask['lot_size_std'].values[0]
                    test_data.loc[town_mask.index, 'rooms_zscore'] = (town_mask['ROOMS'] -
                                                                      agg_mask['rooms_mean'].values[0]) / \
                                                                     agg_mask['rooms_std'].values[0]
                    test_data.loc[town_mask.index, 'beds_zscore'] = (town_mask['BEDS'] - agg_mask['beds_mean'].values[
                        0]) / agg_mask['beds_std'].values[0]
                    test_data.loc[town_mask.index, 'baths_zscore'] = (town_mask['BATHSTOTAL'] -
                                                                      agg_mask['baths_mean'].values[0]) / \
                                                                     agg_mask['baths_std'].values[0]
                    test_data.loc[town_mask.index, 'assessment_zscore'] = (town_mask['ASSESSTOTAL'] -
                                                                           agg_mask['assessment_mean'].values[0]) / \
                                                                          agg_mask['assessment_std'].values[0]
                    test_data.loc[town_mask.index, 'tax_zscore'] = (town_mask['TAXAMOUNT'] -
                                                                    agg_mask['tax_mean'].values[0]) / \
                                                                   agg_mask['tax_std'].values[0]
                    test_data.loc[town_mask.index, 'garages_zscore'] = (town_mask['GARAGECAP'] -
                                                                        agg_mask['garages_mean'].values[0]) / \
                                                                       agg_mask['garages_std'].values[0]
                    test_data.loc[town_mask.index, 'app_fee_zscore'] = (town_mask['APPFEE'] -
                                                                        agg_mask['app_fee_mean'].values[0]) / \
                                                                       agg_mask['app_fee_std'].values[0]
                    test_data.loc[town_mask.index, 'assoc_fee_zscore'] = (town_mask['ASSOCFEE'] -
                                                                          agg_mask['assoc_fee_mean'].values[0]) / \
                                                                         agg_mask['assoc_fee_std'].values[0]
                    test_data.loc[town_mask.index, 'fireplace_zscore'] = (town_mask['FIREPLACES'] -
                                                                          agg_mask['fireplace_mean'].values[0]) / \
                                                                         agg_mask['fireplace_std'].values[0]
                    test_data.loc[town_mask.index, 'sales_rate'] = agg_mask['sales_rate'].values[0]
                    test_data.loc[town_mask.index, 'assess_to_lp_zscore'] = (town_mask['assess_to_lp_ratio'] -
                                                                             agg_mask['assess_to_lp_ratio_mean'].values[
                                                                                 0]) / \
                                                                            agg_mask['assess_to_lp_ratio_std'].values[0]

        return test_data

    @staticmethod
    def expired_reclassification(df: pd.DataFrame):

        temp_data = df.copy()
        years = list(df['YEAR'].unique())
        years.sort()

        for _, year in zip(tqdm(range(len(df['YEAR'].unique())), desc='Years'), years):

            try:
                # Create a dataframe with the year filtered
                year_data = temp_data[temp_data['YEAR'] == year]
                # Get the threshold of the bottom 25% of properties which expired
                reclassification_threshold = year_data[year_data['STATUS_SHORT'] == 'EXP']['DAYSONMARKET'].quantile(
                    0.25)
                # Create a mask to target those specific properties and change their status from WD to XD
                mask = df[(df['YEAR'] == year) & (df['STATUS_SHORT'] == 'WD') & (
                            df['DAYSONMARKET'] >= reclassification_threshold)]
                df.loc[mask.index, 'STATUS_SHORT'] = 'EXP'

            except ValueError as ve:
                print(f'ValueError for Year {year}: {ve}')

        return df

    @staticmethod
    def filter_data(df: pd.DataFrame, phase='stage_one'):

        if phase == 'stage_one':
            filtered_data = df[
                (df['STATUS_SHORT'] != 'WD') & (df['STATUS_SHORT'] != 'XD') & (df['YEAR'] >= 1996)
                & (df['OLP/LP%'] < 60.0) & (df['OLP/LP%'] > -60.0)
                & (df['TAXAMOUNT'] >= 1000) & (df['LISTPRICE'] >= 100)
                & (df['ROOMS'] >= 2) & (df['ROOMS'] <= 25)
                & (df['BEDS'] > 0) & (df['BEDS'] <= 15)
                & (df['BATHSTOTAL'] >= 1) & (df['BATHSTOTAL'] <= 9)
                & (df['ASSESSTOTAL'] <= 4000000) & (df['ASSESSTOTAL'] >= 3000)
                & (df['ASSESSAMOUNTLAND'] <= 700000) & (df['ASSESSAMOUNTBLDG'] <= 4000000)
                & (df['ORIGLISTPRICE'] >= 15000) & (df['SALESPRICE'] <= 5000000)
                & (df['ORIGLISTPRICE'] <= 8000000)
                & (df['GARAGECAP'] <= 15) & (df['APPFEE'] <= 2500)
                & (df['ASSOCFEE'] <= 4000) & (df['LATITUDE'] >= 38)
                & (df['LATITUDE'] <= 43) & (df['FIREPLACES'] <= 7)
                & (df['TAXAMOUNT'] <= 35000) & (df['DAYSONMARKET'] >= 0)
                & (df['DAYSONMARKET'] <= 900)]

            return filtered_data

        else:
            max_age = df['YEAR'].max() - 1600
            filtered_data = df[(df['SQFTAPPROX'] > 500) & (df['YEARBUILT'] > 1600)
                                    & (df['YEARBUILT'] <= df['YEAR'].max())
                                    & (df['AGE_OF_PROPERTY'] > 0) & (df['AGE_OF_PROPERTY'] <= max_age)
                                    & (df['list_price_zscore'] < 5)
                                    & (df['list_to_sales_price_zscore'] > -5) & (
                                                df['list_to_sales_price_zscore'] < 5)
                                    & (df['dom_zscore'] < 5) & (df['sqft_zscore'] < 5)
                                    & (df['age_of_property_zscore'] > -5) & (
                                                df['age_of_property_zscore'] < 5)
                                    & (df['lot_size_zscore'] < 5) & (df['LOTSIZE (SQFT)'] > 0)
                                    & (df['assessment_zscore'] < 5)
                                    & (df['tax_zscore'] < 5) & (df['garages_zscore'] < 5)
                                    & (df['rooms_zscore'] < 5) & (df['beds_zscore'] < 5)
                                    & (df['baths_zscore'] < 5) & (df['fireplace_zscore'] < 5)]

            return filtered_data

    @staticmethod
    def fix_housing_style(value):

        if value in ['SeeRem', 'Trailer', -1, '-1']:
            return 'Unknown'

        elif value == 'Ranch,RanchExp':
            return 'RanchExp'

        elif value == 'RanchRas,Ranch':
            return 'RanchRas'

        elif value == 'Victrian':
            return 'Victorian'

        elif value == 'Contemp':
            return 'Contemporary'

        elif value in ['FirstFlr', 'Hi-Rise', 'HighRise', 'MultiFlr', 'OneFloor']:
            return 'Condo'

    @staticmethod
    def fix_lotsize(df):

        temp_df = df.copy()

        for _, row in zip(tqdm(range(len(temp_df)), desc='Rows'), temp_df.iterrows()):
            idx = row[0]
            data = row[1]

            if (data['LOTSIZE (SQFT)'] < 0 or data['LOTSIZE (SQFT)'] == 0) and (
                    data['lotsize_sqft'] < 0 or data['lotsize_sqft'] == 0 or data['lotsize_sqft'] == np.float64(None)):
                df.loc[idx, 'LOTSIZE (SQFT)'] = 0

            elif (data['LOTSIZE (SQFT)'] < 0 or data['LOTSIZE (SQFT)'] == 0) and (
                    data['lotsize_sqft'] > 0 and data['lotsize_sqft'] != np.float64(None)):
                df.loc[idx, 'LOTSIZE (SQFT)'] = data['lotsize_sqft']

            elif (data['LOTSIZE (SQFT)'] != 0) and (
                    data['lotsize_sqft'] == 0 or data['lotsize_sqft'] == np.float64(None)):
                pass

            elif (data['LOTSIZE (SQFT)'] != 0) and (
                    data['lotsize_sqft'] != 0 or data['lotsize_sqft'] != np.float64(None)):

                if data['LOTSIZE (SQFT)'] > data['lotsize_sqft']:
                    df.loc[idx, 'LOTSIZE (SQFT)'] = data['lotsize_sqft']

                else:
                    pass

        return df

    @staticmethod
    def fix_property_valuation(df):

        temp_df = df.copy()

        for _, row in zip(tqdm(range(len(temp_df)), desc='Rows'), temp_df.iterrows()):
            idx = row[0]
            data = row[1]

            theoretical_value = data['ASSESSAMOUNTBLDG'] + data['ASSESSAMOUNTLAND']
            actual_value = data['ASSESSTOTAL']

            if actual_value != theoretical_value:
                df.loc[idx, 'ASSESSTOTAL'] = theoretical_value

        return df

    @staticmethod
    def fix_streetnum(x):

        if '.' in x:
            x = x.split('.')[0]
            return x

        else:
            return x

    @staticmethod
    def fix_string_data(df: pd.DataFrame):

        df['ZIPCODE'] = df['ZIPCODE'].apply(GSMLSCleaning.fix_zipcodes)
        df['STREETNUMDISPLAY'] = df['STREETNUMDISPLAY'].apply(GSMLSCleaning.fix_streetnum)
        df['ADDRESS'] = df['STREETNUMDISPLAY'].str.cat(df['STREETNAME'].str.title(), sep=' ')
        df.insert(2, 'ADDRESS', df.pop('ADDRESS'))
        df.drop(columns=['STREETNUMDISPLAY', 'STREETNAME'], inplace=True)

        return df

    @staticmethod
    def fix_zipcodes(x):

        if '.' in x:
            x = x.split('.')[0]

        if len(x) >= 5:
            return x

        elif len(x) == 4:
            return '0' + x

        elif len(x) == 3:
            return '00' + x

        else:
            return x

    @staticmethod
    def get_mortgage_rates(date_value):
        """Use this function to get the mortgage rates from fred"""

        date = str(date_value).split(" ")[0]
        load_dotenv(get_filepath("env"))
        freds_api_key = os.getenv("FREDS_API")
        freds_api = (f'https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&api_key'
                     f'={freds_api_key}&file_type=json&observation_start={date}&observation_end=9999-12-31')

        print(' ==== ACQUIRING NATIONAL MORTGAGE RATE DATA FROM FREDS API ==== ')
        mortgage_data = requests.get(freds_api)
        mortgage_json = json.loads(mortgage_data.text)
        mortgage_df = pd.DataFrame(mortgage_json['observations'])
        mortgage_df.drop(columns=['realtime_start', 'realtime_end'], inplace=True)
        mortgage_df['date'] = pd.to_datetime(mortgage_df['date'])
        mortgage_df.set_index('date', inplace=True)
        print(' ==== NATIONAL MORTGAGE RATE DATA ACQUIRED  ==== ')

        return mortgage_df

    def get_county_codes(self):

        query = "SELECT * FROM nj_county_codes;"

        df = pd.read_sql_query(query, con=self.ncjar_connection)

        return df

    @staticmethod
    def impute_mortgage_rates(df: pd.DataFrame, mortgage_df: pd.DataFrame):

        print(' ==== IMPUTING MORTGAGE RATE DATA ==== ')
        for start_date, value in mortgage_df.iterrows():

            end_date = start_date + pd.Timedelta('7 days')
            mask = df[(df['LISTDATE'] >= start_date) & (df['LISTDATE'] < end_date)]

            if mask.empty:
                continue

            else:
                df.loc[mask.index, 'MORTGAGE_RATE'] = float(value['value'])

        df.loc[:, 'MORTGAGE_RATE'] = df.loc[:, 'MORTGAGE_RATE'].astype('float64')
        df.insert(17, 'MORTGAGE_RATE', df.pop('MORTGAGE_RATE'))
        print(' ==== MORTGAGE RATE DATA IMPUTATION COMPLETE ==== ')

        return df

    @staticmethod
    def impute_null_data(test_data: pd.DataFrame, agg_data: pd.DataFrame):

        temp_data = test_data.copy()

        print(' ==== IMPUTING NULL/ZERO VALUES ==== ')

        for idx, data in temp_data.iterrows():

            mask = agg_data[(agg_data['YEAR'] == data['YEAR']) & (agg_data['COUNTY'] == data['COUNTY']) & (
                        agg_data['TOWN'] == data['TOWN'])]

            if (data['SQFTAPPROX'] == 0) or (data['SQFTAPPROX'] is None):
                value = agg_data.loc[mask.index, 'sq_ft_mean'].values[0]
                test_data.loc[idx, 'SQFTAPPROX'] = math.floor(value)

            if (data['LOTSIZE (SQFT)'] == 0) or (data['LOTSIZE (SQFT)'] is None) or (data['LOTSIZE (SQFT)'] < 0):
                value = agg_data.loc[mask.index, 'lot_size_mean'].values[0]
                test_data.loc[idx, 'LOTSIZE (SQFT)'] = value

            if (data['YEARBUILT'] == 0) or (data['YEARBUILT'] is None):
                value = agg_data.loc[mask.index, 'year_built_mean'].values[0]
                test_data.loc[idx, 'YEARBUILT'] = math.floor(value)

            if (data['AGE_OF_PROPERTY'] == 0) or (data['AGE_OF_PROPERTY'] is None):
                value = agg_data.loc[mask.index, 'age_of_property_mean'].values[0]
                test_data.loc[idx, 'AGE_OF_PROPERTY'] = math.floor(value)

        print(' ==== NULL/ZERO VALUE IMPUTATION COMPLETE ==== ')

        return test_data

    def initial_data_loading(self, phase: str):

        if phase == 'stage_one':

            change_dtype_dict = {
                'LATITUDE': 'float64',
                'LONGITUDE': 'float64',
                'BEDS': 'float64',
                'SALESPRICE': 'float64',
                'ORIGLISTPRICE': 'float64',
                'FIREPLACES': 'float64'
            }

            query = f"""
                SELECT * FROM {self.table_name}
                WHERE "PYSPARK_PROCESSED" = false;
            """

            print(f' ==== QUERYING INITIAL DATA FROM {self.table_name} ==== ')
            df = pd.read_sql_query(query, con=self.gsmls_connection)
            df = df.astype(change_dtype_dict)
            df = df[(df['OLP/LP%'] < np.inf)]
            df.drop_duplicates(subset=['MLSNUM'], keep='last', inplace=True)
            print(f' ==== QUERYING COMPLETE ==== ')

            return df

        elif phase == 'stage_three':

            query = f"""
                SELECT * FROM gsmls_imputed_data
                WHERE pyspark_processed = false       
            ;"""
            print(f' ==== QUERYING IMPUTED GSMLS DATA==== ')
            df = pd.read_sql_query(query, con=self.tax_connection)

            print(f' ==== QUERYING COMPLETE ==== ')

            return df

    @staticmethod
    def load_stage_two_data(df=None):

        filepath = "/data/stage_two/gsmls_imputed_data.parquet"

        return pd.read_parquet(filepath)

    @staticmethod
    def object_to_str(df: pd.DataFrame):

        df[df.select_dtypes("object").columns] = df.select_dtypes("object").astype("string")

        return df

    @staticmethod
    def parse_date(value):
        """Use this function to fix the month and year values"""

        date = str(value).split('-')
        month = int(date[1])
        year = int(date[0])

        return month, year

    @staticmethod
    def pre_cleaning(df: pd.DataFrame):

        temp_data = df.copy()
        style_list = ['SeeRem', 'Ranch,RanchExp', 'RanchRas,Ranch', 'Trailer', 'Victrian',
                      'Contemp', 'FirstFlr', 'Hi-Rise', 'HighRise', 'MultiFlr', 'OneFloor', -1, '-1']

        for _, row in zip(tqdm(range(len(df)), desc='Rows'), temp_data.iterrows()):

            idx = row[0]
            data = row[1]

            try:
                # Parse the date for the Expired and Withdrawn data and impute the month and year
                if data['STATUS_SHORT'] not in ['S', 'SD']:
                    month, year = GSMLSCleaning.parse_date(data['LISTDATE'])
                    df.loc[idx, 'MONTH'] = month
                    df.loc[idx, 'YEAR'] = year

                # Fix the house styles of a few data points
                if data['STYLEPRIMARY_SHORT'] in style_list:
                    df.loc[idx, 'STYLEPRIMARY_SHORT'] = GSMLSCleaning.fix_housing_style(data['STYLEPRIMARY_SHORT'])

                if data['STATUS_SHORT'] in ['X', 'XD']:
                    df.loc[idx, 'STATUS_SHORT'] = 'EXP'

                elif data['STATUS_SHORT'] == 'S':
                    df.loc[idx, 'STATUS_SHORT'] = 'SD'

            except IndexError as ie:
                # No LISTDATE is available
                print(f'Index Error @ {data["ADDRESS"]} - {data["LISTDATE"]}: {ie}')

        return df

    @staticmethod
    def property_valuation(val):

        if val < -1.842767:  # The lower the assess_to_lp_zscore, the more overvalued the property

            return -1

        elif val > 1.724696:  # The higher the assess_to_lp_zscore, the more undervalued the property

            return 1

        else:

            return 0

    @staticmethod
    def recalculate_agg_data(agg_data: pd.DataFrame, test_data: pd.DataFrame):

        print(' ==== RECALCULATING AGGREGATE DATA ==== ')

        for year in test_data['YEAR'].unique():
            print(f' ==== RECALCULATING AGGREGATE DATA FOR {year} ==== ')

            for county in test_data['COUNTY'].unique():

                # Isolate the sold and expired data
                sold_mask = test_data[
                    (test_data['YEAR'] == year) & (test_data['COUNTY'] == county) & (test_data['STATUS_SHORT'] == 'SD')]
                exp_mask = test_data[(test_data['YEAR'] == year) & (test_data['COUNTY'] == county) & (
                            test_data['STATUS_SHORT'] == 'EXP')]

                # Iterate through all the towns to calculate the data
                for town in sold_mask['TOWN'].unique():
                    agg_mask = agg_data[
                        (agg_data['YEAR'] == year) & (agg_data['COUNTY'] == county) & (agg_data['TOWN'] == town)]

                    # Isolate the sold and expired data
                    # Sales_price mean and std needs to be recalculated without the EXP instances skewing the data
                    agg_data.loc[agg_mask.index, 'sold_count'] = len(sold_mask[sold_mask['TOWN'] == town])
                    agg_data.loc[agg_mask.index, 'exp_count'] = len(exp_mask[exp_mask['TOWN'] == town])
                    agg_data.loc[agg_mask.index, 'sales_price_mean'] = sold_mask[sold_mask['TOWN'] == town][
                        'SALESPRICE'].mean()
                    agg_data.loc[agg_mask.index, 'sales_price_std'] = sold_mask[sold_mask['TOWN'] == town][
                        'SALESPRICE'].std()

        agg_data['sales_rate'] = agg_data['sold_count'] / agg_data['transaction_count']

        print(f' ==== AGGREGATE DATA RECALCULATION COMPLETE ==== ')

        return agg_data

    @staticmethod
    def remove_columns(df: pd.DataFrame, phase='stage_one'):

        if phase == 'stage_one':
            skip_columns = ['SP/OLP%', 'LOANTERMS_SHORT', 'BATHSFULLTOTAL', 'BATHSHALFTOTAL', 'LOTSIZE', 'SUBDIVISION',
                            'PENDINGDATE', 'ANTICCLOSEDDATE', 'EXPIREDATE', 'WITHDRAWNDATE', 'OWNERSHIP_SHORT',
                            'FLOODZONE', 'ZONING', 'COMPBUY', 'COMPSELL', 'COMPTRANS', 'LISTTYPE_SHORT', 'OFFICELIST',
                            'OFFICESELL', 'OFFICESELLNAME', 'AGENTSELLNAME', 'SELLERNAME', 'AGENTLIST', 'AGENTSELL',
                            'BUSRELATION_SHORT', 'MLS', 'PARKNBRAVAIL']

            df = df[[i for i in df.columns if i not in skip_columns]]
            print(f' ==== STAGE ONE COLUMN REMOVAL COMPLETE ==== ')

            return df

        elif phase == 'stage_three_a':

            df = df[[i for i in df.columns if i not in ['SQFTAPPROX', 'YEARBUILT', 'TAX_PATTERN']]]
            df.insert(23, 'AGE_OF_PROPERTY', df['YEAR'] - df['year_built'])
            df.insert(21, 'SQFTAPPROX', df.pop('building_sqft'))
            df.insert(22, 'YEARBUILT', df.pop('year_built'))

            print(f' ==== STAGE TWO COLUMN REMOVAL COMPLETE ==== ')

            return df

        elif phase == 'stage_three_b':

            skip_list2 = ['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY',
                          'ZIPCODE', 'NJ_TOWNCODE', 'TOWNCODE', 'COUNTYCODE', 'BLOCKID',
                          'LOTID', 'TAXID', 'SUBDIVISION', 'CONDITION', 'QTR',
                          'LISTDATE', 'CLOSEDDATE', 'DAYS_TO_CLOSE', 'ANTIC_CLOSEDATE_DIFF',
                          'LISTING_REMARKS', 'PYSPARK_PROCESSED', 'SCRAPED_DATE']

            df = df[[i for i in df.columns if i not in skip_list2]]
            print(f' ==== STAGE THREE COLUMN REMOVAL COMPLETE ==== ')

            return df

    @staticmethod
    def remove_inf_na(test_data: pd.DataFrame):

        test_data_len = len(test_data)
        true_test_data = test_data[(~test_data['SQFTAPPROX'].isna()) & (~test_data['YEARBUILT'].isna()) & (
            ~test_data['AGE_OF_PROPERTY'].isna()) & (~test_data['MORTGAGE_RATE'].isna())
                                   & (~test_data['list_to_sales_price_zscore'].isna()) & (
                                       ~test_data['sqft_zscore'].isna()) & (
                                       ~test_data['age_of_property_zscore'].isna()) & (
                                       ~test_data['dom_zscore'].isna()) & (~test_data['year_built_zscore'].isna()) & (
                                       ~test_data['lot_size_zscore'].isna())
                                   & (test_data['list_to_sales_price_zscore'] != np.inf) & (
                                               test_data['sqft_zscore'] != np.inf) & (
                                               test_data['age_of_property_zscore'] != np.inf) & (
                                               test_data['dom_zscore'] != np.inf) & (
                                               test_data['year_built_zscore'] != np.inf) & (
                                               test_data['lot_size_zscore'] != np.inf)
                                   & (test_data['list_to_sales_price_zscore'] != -np.inf) & (
                                               test_data['sqft_zscore'] != -np.inf) & (
                                               test_data['age_of_property_zscore'] != -np.inf) & (
                                               test_data['dom_zscore'] != -np.inf) & (
                                               test_data['year_built_zscore'] != -np.inf) & (
                                               test_data['lot_size_zscore'] != -np.inf)]

        true_test_data_len = len(true_test_data)
        null_data = (1 - (true_test_data_len/test_data_len))*100
        print(f' ==== {round(null_data, 2)}% OF DATA WAS NULL/INF AND HAS BEEN REMOVED ==== ')

        return true_test_data

    @staticmethod
    def reorder_zscore_cols(test_data: pd.DataFrame):

        column_names = {
            'LISTPRICE': 'list_price_zscore',
            'list_price_zscore': 'list_to_sales_price_zscore',
            'DAYSONMARKET': 'dom_zscore',
            'SQFTAPPROX': 'sqft_zscore',
            'AGE_OF_PROPERTY': 'age_of_property_zscore',
            'YEARBUILT': 'year_built_zscore',
            'LOTSIZE (SQFT)': 'lot_size_zscore',
            'SALESPRICE': 'sales_rate',
            'ROOMS': 'rooms_zscore',
            'BEDS': 'beds_zscore',
            'BATHSTOTAL': 'baths_zscore',
            'ASSESSTOTAL': 'assessment_zscore',
            'TAXAMOUNT': 'tax_zscore',
            'GARAGECAP': 'garages_zscore',
            'APPFEE': 'app_fee_zscore',
            'ASSOCFEE': 'assoc_fee_zscore',
            'FIREPLACES': 'fireplace_zscore',
            'ASSESSTOTAL': 'assess_to_lp_zscore'
        }

        for k, v in column_names.items():
            target_index = test_data.columns.get_loc(k)
            test_data.insert(target_index + 1, v, test_data.pop(v))

        return test_data

    def save_cleaned_data(self, df: pd.DataFrame):

        df.to_sql('cleaned_data_for_dnn', con=self.tax_connection, if_exists='append', index=False)
        print(f' ==== NEW DATA HAS BEEN APPENDED TO cleaned_data_for_dnn ==== ')
        # df.to_sql('temp_data_for_dnn', con=self.tax_connection, if_exists='replace', index=False)

    @staticmethod
    def update_pyspark_processed(mls_list: list, property_type: str):

        data_dict = {
            'RES': {
                'raw_data': {'table_name': 'res_properties', 'database': 'gsmls',
                             'col': 'PYSPARK_PROCESSED', 'mls_col': ""},
                'clean_data': {'table_name': 'gsmls_imputed_data',
                               'database': 'nj_tax_assessor', 'col': 'pyspark_processed'}
            },
            'MUL': {},
            'LND': {}
        }

        for data in [data_dict[property_type]['raw_data'], data_dict[property_type]['clean_data']]:

            _, properties = create_postgres_connection('psycopg2', data['database'])
            conn = psycopg2.connect(**properties)
            print(f" ==== POSTGRESQL + PSYCOPG2 CONNECTION TO {data['database']} WAS SUCCESSFUL ==== ")

            if data["table_name"] in ['res_properties']:
                query = f'''
                    UPDATE {data["table_name"]}
                    SET "{data["col"]}" = true
                    WHERE "MLSNUM" IN {tuple(mls_list)}
                '''
            else:
                query = f'''
                    UPDATE {data["table_name"]}
                    SET {data["col"]} = true
                    WHERE mlsnum IN {tuple(mls_list)}
                '''

            with conn:
                with conn.cursor() as cur:
                    cur.execute(query)

            print(f" ==== SUCCESSFULLY LABELED THE MLSNUM(s) AS PYSPARK PROCESSED IN {data['table_name']}==== ")
            conn.close()

    def verify_dtypes(self, df: pd.DataFrame):

        query = "SELECT * FROM cleaned_data_for_dnn LIMIT 1;"
        dtype_df = pd.read_sql_query(query, con=self.tax_connection)

        for col in dtype_df.columns:
            dtype = str(dtype_df[col].dtype)
            df.loc[:, col] = df.loc[:, col].astype(dtype)

        return df









