import os
import json
import pandas as pd
import numpy
import re
import traceback
import time
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta
from gsmls.utility_func import create_sql_engine, create_kafka_producer
from gsmls.utility_func import create_kafka_consumer, logger_decorator, get_filepath
from kafka.errors import KafkaTimeoutError, MessageSizeTooLargeError, RebalanceInProgressError
from gsmls.RealEstateImages import RealEstateImages
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.exc import DatabaseError


class KafkaGSMLSConsumer:

    def __init__(self):
        self.connection = create_sql_engine('gsmls', True)
        self.producer = create_kafka_producer(client_id="data_producer")
        self.consumer = create_kafka_consumer("data_consumer", "data_consumer")
        self.prop_dict = {
            'RES': {'topic': 'res_properties', 'functions': 14, 'clean_type': KafkaGSMLSConsumer.res_property_cleaning},
            'MUL': {'topic': 'mul_properties', 'functions': 13, 'clean_type': KafkaGSMLSConsumer.mul_property_cleaning},
            'LND': {'topic': 'lnd_properties', 'functions': 12, 'clean_type': KafkaGSMLSConsumer.lnd_property_cleaning},
            'RNT': {'topic': 'rnt_properties', 'functions': 8, 'clean_type': KafkaGSMLSConsumer.rnt_property_cleaning},
            'TAX': {'topic': 'tax_properties_new', 'functions': 6, 'clean_type': KafkaGSMLSConsumer.tax_property_cleaning},
            'IMAGES': {'topic': 'prop_images', 'functions': 0, 'clean_type': None},
        }

    @staticmethod
    def baths_empty(df_var, prop_type):

        """
        REFACTOR
        """

        if prop_type in ['RES', 'MUL', 'RNT']:

            baths_empty = df_var[df_var['BATHSTOTAL'] == 0.0]

            if not baths_empty.empty:

                for idx, row in baths_empty.iterrows():

                    df_var.loc[idx, 'BATHSTOTAL'] = row['BATHSFULLTOTAL']

        return df_var

    @staticmethod
    def calculate_dates(df_var, prop_type, update_bar):

        if prop_type in ['RES', 'MUL', 'LND']:
            df_var['YEAR'] = df_var['CLOSEDDATE'].apply(KafkaGSMLSConsumer.parse_year)
            df_var['MONTH'] = df_var['CLOSEDDATE'].apply(KafkaGSMLSConsumer.parse_month)
            df_var['LISTDATE'] = pd.to_datetime(df_var['LISTDATE'], errors='coerce')
            df_var['CLOSEDDATE'] = pd.to_datetime(df_var['CLOSEDDATE'], errors='coerce')
            df_var['PENDINGDATE'] = pd.to_datetime(df_var['PENDINGDATE'], errors='coerce')
            df_var['ANTICCLOSEDDATE'] = pd.to_datetime(df_var['ANTICCLOSEDDATE'], errors='coerce')
            df_var['SCRAPED_DATE'] = pd.to_datetime(df_var['SCRAPED_DATE'], errors='coerce')
            df_var['DAYS_TO_CLOSE'] = df_var['CLOSEDDATE'] - df_var['PENDINGDATE']
            df_var['ANTIC_CLOSEDATE_DIFF'] = df_var['CLOSEDDATE'] - df_var['ANTICCLOSEDDATE']
            # Stuck these transformations here to not make a new function
            df_var['SP/LP%'] = df_var['SP/LP%'].astype('float64')
            df_var['SP/LP%'] = df_var['SP/LP%'] - 100.0

            if prop_type == 'RES':
                df_var = df_var.rename(columns={'OWNERNAME': 'SELLERNAME', 'SUBPROPTYPE': 'SUBPROPTYPE_SFH'})
            else:
                df_var = df_var.rename(columns={'OWNERNAME': 'SELLERNAME'})

        elif prop_type == 'RNT':
            df_var['YEAR'] = df_var['RENTEDDATE'].apply(KafkaGSMLSConsumer.parse_year)
            df_var['MONTH'] = df_var['RENTEDDATE'].apply(KafkaGSMLSConsumer.parse_month)
            df_var['RENTEDDATE'] = pd.to_datetime(df_var['RENTEDDATE'], errors='coerce')
            df_var['SCRAPED_DATE'] = pd.to_datetime(df_var['SCRAPED_DATE'], errors='coerce')
            # Stuck these transformations here to not make a new function
            df_var['RP/LP%'] = df_var['RP/LP%'].astype('float64')
            df_var['RP/LP%'] = df_var['RP/LP%'] - 100.0

        elif prop_type == 'TAX':
            df_var['MTGDATE'] = pd.to_datetime(df_var['MTGDATE'], errors='coerce')
            df_var['RECDATE'] = pd.to_datetime(df_var['RECDATE'], errors='coerce')
            df_var['DATEMODIFIED'] = pd.to_datetime(df_var['DATEMODIFIED'], errors='coerce')
            df_var['PRIORSALEDATE'] = pd.to_datetime(df_var['PRIORSALEDATE'], errors='coerce')
            df_var['SALEDATE'] = pd.to_datetime(df_var['SALEDATE'], errors='coerce')
            df_var['DEDATE'] = pd.to_datetime(df_var['DEDATE'], errors='coerce')
            df_var['LASTMODDATE'] = pd.to_datetime(df_var['LASTMODDATE'], errors='coerce')
            df_var['PREVOWN_POSS_TIME'] = df_var['SALEDATE'] - df_var['PRIORSALEDATE']

        update_bar.update(1)
        return df_var

    @staticmethod
    def change_datatypes(df_var, prop_type, update_bar):
        """
        REFACTOR
        """

        if prop_type == 'RES':
            # fill_na() function turns this col to string. TypeError thrown if string value is explictly tried to
            # change to Boolean type, even if it's "True" or "False:
            df_var["PYSPARK_PROCESSED"] = False
            update_bar.update(1)
            return df_var.astype({'TOWNCODE': 'int64', 'ASSESSAMOUNTBLDG': 'float64', 'APPFEE': 'float64', 'YEAR': 'int64',
                                  'ASSESSAMOUNTLAND': 'float64', 'ASSESSTOTAL': 'float64', 'QTR': 'int64',
                                  'TAXAMOUNT': 'float64', 'YEARBUILT': 'float64', 'SQFTAPPROX': 'float64',
                                  'ORIGLISTPRICE': 'int64', 'LISTPRICE': 'int64', 'SALESPRICE': 'int64',
                                  'PARKNBRAVAIL': 'int64', 'PYSPARK_PROCESSED': 'boolean'})

        elif prop_type == 'MUL':
            df_var["PYSPARK_PROCESSED"] = False
            update_bar.update(1)
            return df_var.astype({'TOWNCODE': 'int64', 'ASSESSAMOUNTBLDG': 'float64', 'YEAR': 'int64',
                                  'ASSESSAMOUNTLAND': 'float64', 'ASSESSTOTAL': 'float64', 'QTR': 'int64',
                                  'TAXAMOUNT': 'float64', 'YEARBUILT': 'float64', 'SQFTBLDG': 'float64',
                                  'INCOMEGROSSOPERATING': 'float64', 'EXPENSEOPERATING': 'float64',
                                  'INCOMENETOPERATING': 'float64', 'ORIGLISTPRICE': 'int64', 'LISTPRICE': 'int64',
                                  'SALESPRICE': 'int64', 'PARKNBRAVAIL': 'int64', 'PYSPARK_PROCESSED': 'boolean'})

        elif prop_type == 'LND':
            df_var["PYSPARK_PROCESSED"] = False
            update_bar.update(1)
            return df_var.astype({'TOWNCODE': 'int64', 'ASSESSAMOUNTBLDG': 'float64', 'YEAR': 'int64',
                                  'ASSESSAMOUNTLAND': 'float64', 'ASSESSTOTAL': 'float64', 'QTR': 'int64',
                                  'TAXAMOUNT': 'float64', 'ORIGLISTPRICE': 'int64', 'LISTPRICE': 'int64',
                                  'SALESPRICE': 'int64', 'PYSPARK_PROCESSED': 'boolean'})

        elif prop_type == 'RNT':
            df_var["PYSPARK_PROCESSED"] = False
            update_bar.update(1)
            return df_var.astype({'TOWNCODE': 'int64', 'YEAR': 'int64', 'QTR': 'int64', 'BEDS': 'int64',
                                  'YEARBUILT': 'float64', 'SQFTAPPROX': 'float64', 'RENTMONTHPERLSE': 'int64',
                                  'GARAGECAP': 'int64', 'LP': 'int64', 'RENTPRICEORIG': 'int64',
                                  'LENGTHOFLEASE': 'int64', 'PYSPARK_PROCESSED': 'boolean'})

        elif prop_type == 'TAX':
            update_bar.update(1)
            return df_var.astype({'ACRES': 'float64', 'PREVOWN_POSS_TIME': 'string', 'YEARBUILT': 'int64',
                                  'ASSESSMENT1': 'int64', 'ASSESSMENT2': 'int64', 'ASSESSMENT3': 'int64',
                                  'TOTALASSESSMENT': 'int64'})

    @staticmethod
    def checkpoint(df_var, topic):

        base_path = get_filepath('backups')
        filepath = os.path.join(base_path, f'{topic}.xlsx')

        print(f' ==== PROCESSING ERROR OCCURRED. CREATING DATA CHECKPOINT FOR {topic} ==== ')
        df_var.to_excel(filepath, index=False)

    @staticmethod
    def column_indexer(targ_col: str, cols: pd.Index, indexes: numpy.array):

        col_index = {}

        for col, idx in zip(cols, indexes):
            col_index[col] = idx

        return int(col_index[targ_col])

    @staticmethod
    def combine_listing_remarks(df_var, update_bar):

        df_var['LISTING_REMARKS'] = (df_var['REMARKSPUBLIC']
                                     .str.cat([df_var['REMARKSAGENT'], df_var['SHOWSPECIAL']], na_rep='_', sep='. '))

        update_bar.update(1)
        return df_var

    def consume_data(self, prop_type):

        df_list = []
        keys_list = []
        start_time = datetime.now()
        progress_bar = tqdm(range(len(df_list)), desc='New Data Found', colour='green', position=1)

        while True:

            try:
                new_data = self.consumer.poll(timeout_ms=4000)

                if new_data:
                    sub_list, key_list = KafkaGSMLSConsumer.extract_messages(new_data)

                    if isinstance(sub_list, list):
                        df_list.extend(sub_list)
                        keys_list.extend(key_list)
                        progress_bar.update(len(sub_list))
                    else:
                        pass

                elif ((datetime.now() - start_time) < timedelta(minutes=30)) and len(df_list) == 0:

                    print(' === WAITING FOR NEW DATA === ')
                    print(f' ==== CURRENT TIME LAPSE: {datetime.now() - start_time} ====')
                    continue

                elif ((datetime.now() - start_time) < timedelta(minutes=30)) and len(df_list) > 0:

                    return self.prepare_data(df_list, prop_type), keys_list

                else:
                    print(' === NO NEW DATA. KAFKA DATA CONSUMPTION COMPLETE === ')
                    return None, None

            except ValueError:
                pass

    @staticmethod
    def convert_lot_size(df_var, update_bar):

        try:
            df_var['ACRES'] = df_var['ACRES'].astype('float64')
            df_var['LOTSIZE (SQFT)'] = df_var['ACRES'] * 43560
        except ValueError:
            df_var['ACRES'] = pd.to_numeric(df_var['ACRES'], errors='coerce')
            df_var['LOTSIZE (SQFT)'] = df_var['ACRES'] * 43560

        temp_df_ = df_var.copy()

        for idx, row in temp_df_.iterrows():

            try:
                if row['LOTSIZE (SQFT)'] == 0.0:
                    value = row['LOTSIZE']
                    df_var.loc[idx, 'LOTSIZE (SQFT)'] = KafkaGSMLSConsumer.fix_lotsize(value)
            except TypeError:
                df_var.loc[idx, 'LOTSIZE (SQFT)'] = 0.0

        update_bar.update(1)
        return df_var

    @staticmethod
    def create_final_df(df, prop_type, clean_type, cleaning_bar):

        if prop_type != 'IMAGES':
            return df.pipe(clean_type, prop_type=prop_type, update_bar=cleaning_bar)

        else:
            return df

    @staticmethod
    def delete_checkpoint(topic):

        base_path = get_filepath('backups')
        filepath = os.path.join(base_path, f'{topic}.xlsx')

        if os.path.isfile(filepath):
            print(f' ==== DELETING CHECKPOINT: {filepath} ==== ')
            os.remove(filepath)

    @staticmethod
    def detect_unnamed_col(cols: list):

        col_pattern = re.compile(r'UNNAMED: \d{2,4}')
        del_cols = []

        for i in cols:
            if col_pattern.search(i) is not None:
                del_cols.append(i)

        if len(del_cols) > 0:
            return del_cols
        else:
            return None

    @staticmethod
    def drop_columns(df_var, prop_type, update_bar):
        """
        REFACTOR
        """

        cols_list = df_var.columns

        if prop_type == 'RES':
            update_bar.update(1)
            return df_var.drop(columns=['ACRES', 'REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'DRIVEWAYDESC_SHORT',
                                         'COOLSYSTEM_SHORT', 'FLOORS_SHORT', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                                         'ROOF_SHORT', 'SEWER_SHORT', 'SIDING_SHORT', 'EXTERIOR_SHORT', 'BASEDESC_SHORT',
                                        'STYLE_SHORT', 'TAXRATE', 'TAXYEAR', 'WATER_SHORT', 'UTILITIES_SHORT',
                                        'BASEMENT_SHORT', 'IMAGES', 'PROP_CLASS'])

        elif prop_type == 'MUL':
            update_bar.update(1)
            return df_var.drop(columns=['ACRES', 'REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'DRIVEWAYDESC_SHORT',
                                        'COOLSYSTEM_SHORT', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                                        'ROOF_SHORT', 'SEWER_SHORT', 'SIDING_SHORT', 'EXTERIOR_SHORT', 'BASEDESC_SHORT',
                                        'UNITSTYLE_SHORT', 'TAXRATE', 'TAXYEAR', 'WATER_SHORT', 'UTILITIES_SHORT',
                                        'BASEMENT_SHORT', 'IMAGES', 'PROP_CLASS'])

        elif prop_type == 'LND':
            update_bar.update(1)
            return df_var.drop(columns=['ACRES', 'REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'TAXRATE',
                                        'TAXYEAR', 'BUILDINGSINCLUDED_SHORT', 'CURRENTUSE_SHORT', 'DEVSTATUS_SHORT',
                                        'IMPROVEMENTS_SHORT', 'LOTDESC_SHORT','ROADSURFACEDESC_SHORT', 'SITEPARTICULARS_SHORT',
                                        'SEWERINFO_SHORT', 'WATERINFO_SHORT', 'ZONINGDESC_SHORT', 'PROP_CLASS'])

        if prop_type == 'RNT':
            update_bar.update(1)
            return df_var.drop(columns=['REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'DRIVEWAYDESC_SHORT',
                                         'COOLSYSTEM_SHORT', 'FLOORS_SHORT', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
                                         'SEWER_SHORT', 'BASEDESC_SHORT', 'WATER_SHORT', 'UTILITIES_SHORT',
                                        'BASEMENT_SHORT', 'TENANTPAYS_SHORT', 'RENTINCLUDES_SHORT', 'IMAGES', 'PROP_CLASS'])

        if prop_type == 'TAX':

            target_cols = KafkaGSMLSConsumer.detect_unnamed_col(list(cols_list))
            if target_cols is not None:
                return df_var.drop(columns=target_cols)
            else:
                return df_var

    @staticmethod
    def escape_illegal_char(df_var, prop_type, update_bar):
        """
        REFACTOR
        """

        illegal_pattern = ('\x00|\x01|\x02|\x03|\x04|\x05|\x06|\x07|\x08|\x0b|\x0c|\x0e|\x0f|\x10|\x11|\x12|\x13|\x14'
                           '|\x15|\x16|\x17|\x18|\x19|\x1a|\x1b|\x1c|\x1d|\x1e|\x1f')

        if prop_type in ['RES', 'MUL', 'LND']:
            target_columns = ['STREETNUMDISPLAY', 'STREETNAME', 'LOTID', 'BLOCKID',
                              'LATITUDE', 'LONGITUDE', 'LOTSIZE', 'SUBDIVISION', 'OFFICESELLNAME', 'AGENTSELLNAME',
                              'SELLERNAME', 'LISTING_REMARKS']

        elif prop_type == 'RNT':
            target_columns = ['STREETNUMDISPLAY', 'STREETNAME', 'LOTID', 'BLOCKID',
                              'LATITUDE', 'LONGITUDE', 'SUBDIVISION']

        elif prop_type == 'TAX':
            target_columns = ['MCR', 'LOCNUM', 'LOCDIR', 'LOCSTREET', 'LOCMODE', 'LOCCITY', 'PROPERTYDESC', 'OWNER',
                              'MAILNUM', 'MAILDIR', 'MAILSTREET', 'MAILMODE', 'MAILCITY', 'PRIOROWNER']

        for column in target_columns:
            try:
                df_var[column] = df_var[column].str.replace(illegal_pattern, '', regex=True)
            except KeyError as KE:
                print(f'KeyError occurred in escape_illegal_cha(): {KE}')
                continue

        update_bar.update(1)
        return df_var

    @staticmethod
    def extract_messages(new_messages):

        sub_list = []
        key_list = []

        for partition_obj, messages_list in new_messages.items():

            for record in messages_list:

                try:
                    json_obj = json.loads(json.loads(record.value))

                    df = pd.DataFrame(data=json_obj['data'],
                                        index=json_obj['index'],
                                        columns=json_obj['columns'])
                    sub_list.append(df)
                    if record.key != 'null':
                        key_list.append(record.key)

                except json.decoder.JSONDecodeError:

                    pass
        if len(sub_list) > 0:
            return sub_list, key_list
        else:
            return None, None

    @staticmethod
    def fill_na_values(df_var, prop_type, update_bar):

        df_var = df_var.astype('string')

        datatype_dict = {'ACRES': ['0.0', 'string'],
                        'ADDITIONALLOTS': ['N', 'string'],
                        'ADDJUDGEMENTS': ['N', 'string'],
                        'ADDLMTGHOLDER': ['N', 'string'],
                        'AGENTLIST': ['000000', 'string'],
                        'AGENTSELL': ['000000', 'string'],
                        'AGENTSELLNAME': ['NOT AVAILABLE', 'string'],
                        'AMENITIES_SHORT': ['Unknown', 'string'],
                        'ANTICCLOSEDDATE': ['00/00/0000 00:00:00', 'string'],
                        'APPFEE': ['0.0', 'string'],
                        'APPLIANCES_SHORT': ['Unknown', 'string'],
                        'ASSESSAMOUNTBLDG': ['0.0', 'string'],
                        'ASSESSAMOUNTLAND': ['0.0', 'string'],
                        'ASSESSMENT1': ['0', 'string'],
                        'ASSESSMENT2': ['0', 'string'],
                        'ASSESSMENT3': ['0', 'string'],
                        'ASSESSTOTAL': ['0.0', 'string'],
                        'ASSOCFEE': ['0.0', 'float64'],
                        'ATTONEY': ['N', 'string'],
                        'AUTOROW': ['0', 'int64'],
                        'AVAILABLE_SHORT': ['Unknown', 'string'],
                        'BANKCODE': ['0', 'string'],
                        'BASEDESC_SHORT': ['None', 'string'],
                        'BASEMENT_SHORT': ['N', 'string'],
                        'BATHSFULLTOTAL': ['0.0', 'float64'],
                        'BATHSHALFTOTAL': ['0.0', 'float64'],
                        'BATHSTOTAL': ['0.0', 'float64'],
                        'BEDS': ['0', 'int64'],
                        'BLOCKID': ['0', 'string'],
                        'BLOCKSUFFIX': ['00', 'string'],
                        'BUILDINGCLASSCODE': ['00', 'string'],
                        'BUILDINGDESC': ['Unknown', 'string'],
                        'BUILDINGSINCLUDED_SHORT': ['Unknown', 'string'],
                        'BUSRELATION_SHORT': ['Unknown', 'string'],
                        'CALCTAXAMT':['0.0', 'float64'],
                        'CBLOCK': ['00', 'string'],
                        'CITYCODE': ['0', 'int64'],
                        'CLOSEDDATE': ['00/00/0000 00:00:00', 'string'],
                        'COMPBUY': ['None', 'string'],
                        'COMPSELL': ['None', 'string'],
                        'COMPTRANS': ['None', 'string'],
                        'COOLSYSTEM_SHORT': ['Unknown', 'string'],
                        'COUNTY': ['Unknown', 'string'],
                        'COUNTYCODE': ['00', 'string'],
                        'CTRACT': ['00', 'string'],
                        'CURRENTUSE_SHORT': ['Unknown', 'string'],
                        'DATEMODIFIED': ['00/00/0000 00:00:00', 'string'],
                        'DAYSONMARKET': ['0.0', 'float64'],
                        'DEDATE': ['00/00/0000 00:00:00', 'string'],
                        'DEDUCTIONAMOUNT': ['0.0', 'string'],
                        'DEDUCTIONS': ['0.0', 'string'],
                        'DEEDBOOK': ['Unknown', 'string'],
                        'DEEDPAGE': ['Unknown', 'string'],
                        'DEVRESTRICT_SHORT': ['Unknown', 'string'],
                        'DEVSTATUS_SHORT': ['Unknown', 'string'],
                        'DOCKET': ['N', 'string'],
                        'DOI': ['N', 'string'],
                        'DRIVEWAYDESC_SHORT': ['Unknown', 'string'],
                        'EASEMENT_SHORT': ['N', 'string'],
                        'EQVALUE': ['0.0', 'float64'],
                        'EXPENSEOPERATING': ['0.0', 'string'],
                        'EXPENSESINCLUDE_SHORT': ['None', 'string'],
                        'EXPIREDATE': ['00/00/0000 00:00:00', 'string'],
                        'FBANK': ['N', 'string'],
                        'FIPS': ['00000', 'string'],
                        'FIREPLACES': ['0', 'int64'],
                        'FIRSTNAME1': ['Unknown', 'string'],
                        'FIRSTNAME2': ['Unknown', 'string'],
                        'FLOORS_SHORT': ['Unknown', 'string'],
                        'FORCLOSEAMT': ['0.0', 'string'],
                        'FULLLOC': ['Unknown', 'string'],
                        'FULLMAIL': ['Unknown', 'string'],
                        'FULLRATIOYEAR': ['0', 'string'],
                        'FULLTAXYEAR': ['0', 'string'],
                        'FURNISHINFO_SHORT': ['Unknown', 'string'],
                        'GARAGECAP': ['0.0', 'float64'],
                        'GSCOUNTYCODE': ['0', 'string'],
                        'HEATSRC_SHORT': ['Unknown', 'string'],
                        'HEATSYSTEM_SHORT': ['Unknown', 'string'],
                        'IMPROVEMENTS_SHORT': ['None', 'string'],
                        'INCOMEGROSSOPERATING': ['0.0', 'string'],
                        'INCOMENETOPERATING': ['0.0', 'string'],
                        'LASTMODDATE': ['00/00/0000 00:00:00', 'string'],
                        'LASTNAME1': ['Unknown', 'string'],
                        'LASTNAME2': ['Unknown', 'string'],
                        'LATITUDE': ['0E-20', 'string'],
                        'LAUNDRYFAC': ['Unknown', 'string'],
                        'LCR': ['0', 'string'],
                        'LDPC': ['0', 'string'],
                        'LDPCDIGIT': ['0', 'string'],
                        'LEASETERMS_SHORT': ['Unknown', 'string'],
                        'LENGTHOFLEASE': ['0.0', 'float64'],
                        'LISTDATE': ['00/00/0000 00:00:00', 'string'],
                        'LISTPRICE': ['0', 'int64'],
                        'LISTTYPE_SHORT': ['Unknown', 'string'],
                        'LOANTERMS': ['Unknown', 'string'],
                        'LOANTERMS_SHORT': ['Unknown', 'string'],
                        'LOCATION_SHORT': ['Unknown', 'string'],
                        'LOCCITY': ['Unknown', 'string'],
                        'LOCDIR': ['Unknown', 'string'],
                        'LOCMODE': ['Unknown', 'string'],
                        'LOCNUM': ['00', 'string'],
                        'LOCNUM_NUM': ['00', 'string'],
                        'LOCPOST': ['00', 'string'],
                        'LOCSTATE': ['Unknown', 'string'],
                        'LOCSTREET': ['Unknown', 'string'],
                        'LOCUNIT': ['Unknown', 'string'],
                        'LOCZIP': ['00000', 'string'],
                        'LONGITUDE': ['0E-20', 'string'],
                        'LOT': ['0', 'int64'],
                        'LOTDESC_SHORT': ['None', 'string'],
                        'LOTID': ['0', 'string'],
                        'LOTSIZE': ['0x0', 'string'],
                        'LOTSUFFIX': ['00', 'string'],
                        'LP': ['0.0', 'float64'],
                        'MAILCITY': ['Unknown', 'string'],
                        'MAILCITYSTATE': ['Unknown', 'string'],
                        'MAILCITYSTATEZIP': ['Unknown', 'string'],
                        'MAILCITYSTATEZIP4': ['Unknown', 'string'],
                        'MAILDIR': ['Unknown', 'string'],
                        'MAILMODE': ['Unknown', 'string'],
                        'MAILNUM': ['Unknown', 'string'],
                        'MAILPHONE': ['Unknown', 'string'],
                        'MAILPOST': ['Unknown', 'string'],
                        'MAILSTATE': ['Unknown', 'string'],
                        'MAILSTREET': ['Unknown', 'string'],
                        'MAILUNIT': ['Unknown', 'string'],
                        'MAILZIP': ['00000', 'string'],
                        'MAP': ['00', 'string'],
                        'MCR': ['Unknown', 'string'],
                        'MDPC': ['Unknown', 'string'],
                        'MDPCDIGIT': ['Unknown', 'string'],
                        'MLSCITYCODE': ['0000', 'string'],
                        'MTGAMT': ['Unknown', 'string'],
                        'MTGDATE': ['00/00/0000 00:00:00', 'string'],
                        'MTGHOLDER': ['Unknown', 'string'],
                        'MLSNUM': ['000000', 'string'],
                        'NUMLOTS': ['0', 'int64'],
                        'NUMUNITS': ['0', 'int64'],
                        'OFFICELIST': ['000000', 'string'],
                        'OFFICESELL': ['000000', 'string'],
                        'OFFICESELLNAME': ['NEW JERSEY', 'string'],
                        'OLDPIN': ['000000', 'string'],
                        'ORIGLISTPRICE': ['0.0', 'float64'],
                        'OWNER': ['Unknown', 'string'],
                        'OWNERNAME': ['Not Available', 'string'],
                        'OWNERPAYS_SHORT': ['Unknown', 'string'],
                        'OWNERS': ['1.0', 'float64'],
                        'PARCEL_NO': ['0000-00000-0000-00000-0000', 'string'],
                        'PARKNBRAVAIL': ['0.0', 'float64'],
                        'PENDINGDATE': ['00/00/0000 00:00:00', 'string'],
                        'PERCTEST_SHORT': ['Unknown', 'string'],
                        'PETS_SHORT': ['Unknown', 'string'],
                        'PHONE': ['000-000-0000', 'string'],
                        'POOL_SHORT': ['N', 'string'],
                        'PRERENTREQUIRE_SHORT': ['Unknown', 'string'],
                        'PRIORDEEDBOOK': ['Unknown', 'string'],
                        'PRIORDEEDPAGE': ['Unknown', 'string'],
                        'PRIOROWNER': ['Unknown', 'string'],
                        'PRIORSALEAMT': ['0.0', 'float64'],
                        'PRIORSALEDATE': ['00/00/0000 00:00:00', 'string'],
                        'PROPERTYDESC': ['0x0', 'string'],
                        'PROPERTYTYPEPRIMARY_SHORT': ['Unknown', 'string'],
                        'PROPERTYUSECODE': ['Unknown', 'string'],
                        'PROPSUBTYPERN': ['Unknown', 'string'],
                        'QUALIFIER': ['Unknown', 'string'],
                        'QUARTER': ['Unknown', 'string'],
                        'RANGE_': ['Unknown', 'string'],
                        'RATE': ['0.0', 'float64'],
                        'RATIO': ['0.0', 'float64'],
                        'RATIOYR': ['0', 'int64'],
                        'RECDATE': ['00/00/0000 00:00:00', 'string'],
                        'REMARKSAGENT': ['None', 'string'],
                        'REMARKSPUBLIC': ['None', 'string'],
                        'RENTEDDATE': ['00/00/0000 00:00:00', 'string'],
                        'RENTINCLUDES_SHORT': ['Unknown', 'string'],
                        'RENTMONTHPERLSE': ['0.0', 'float64'],
                        'RENTPRICEORIG': ['0.0', 'float64'],
                        'RENTTERMS_SHORT': ['Unknown', 'string'],
                        'ROADFRONTDESC_SHORT': ['Unknown', 'string'],
                        'ROADSURFACEDESC_SHORT': ['Unknown', 'string'],
                        'ROOMS': ['0.0', 'float64'],
                        'ROWID': ['0', 'string'],
                        'ROWIDNUM': ['0', 'string'],
                        'RP/LP%': ['0', 'int64'],
                        'SALEDATE': ['00/00/0000 00:00:00', 'string'],
                        'SALEPRICE': ['0.0', 'float64'],
                        'SALESPRICE': ['0.0', 'float64'],  # Converting to float type shouldn't throw error later
                        'SECNAME': ['Unknown', 'string'],
                        'SECNUM': ['Unknown', 'string'],
                        'SECTION': ['Unknown', 'string'],
                        'SERVICES_SHORT': ['Unknown', 'string'],
                        'SEWERINFO_SHORT': ['Unknown', 'string'],
                        'SEWER_SHORT': ['Unknown', 'string'],
                        'SHOWSPECIAL': ['None', 'string'],
                        'SITEPARTICULARS_SHORT': ['Unknown', 'string'],
                        'SOILTYPE_SHORT': ['Unknown', 'string'],
                        'SP/LP%': ['0%', 'string'],
                        'SPECIALTAXCODE': ['Unknown', 'string'],
                        'SQFTAPPROX': ['0', 'string'],
                        'SQFTBLDG': ['0', 'string'],
                        'STREETNAME': ['Unknown', 'string'],
                        'STREETNUMDISPLAY': ['0', 'string'],
                        'STYLEPRIMARY_SHORT': ['Unknown', 'string'],
                        'STYLE_SHORT': ['Unknown', 'string'],
                        'SUBDIVISION': ['None', 'string'],
                        'SUBPROPTYPE': ['U', 'string'],
                        'TAXES': ['0.0', 'float64'],
                        'TAXID': ['0000-00000-0000-00000-0000', 'string'],
                        'TAXSYSID': ['000000000', 'string'],
                        'TAXYR': ['0', 'int64'],
                        'TENANTPAYS_SHORT': ['Unknown', 'string'],
                        'TENANTUSEOF_SHORT': ['Unknown', 'string'],
                        'TENLANDCOMM_SHORT': ['Unknown', 'string'],
                        'TOPOGRAPHY_SHORT': ['Unknown', 'string'],
                        'TOTALASSESSMENT': ['0', 'string'],
                        'TOWN': ['Unknown', 'string'],
                        'TOWNCODE': ['0', 'string'],
                        'TOWNSHIP': ['0', 'string'],
                        'UNIT1BATHS': ['0.0', 'string'],
                        'UNIT1BEDS': ['0', 'string'],
                        'UNIT1OWNERTENANTPAYS_SHORT': ['None', 'string'],
                        'UNIT1ROOMS': ['0', 'string'],
                        'UNIT2BATHS': ['0.0', 'string'],
                        'UNIT2BEDS': ['0', 'string'],
                        'UNIT2OWNERTENANTPAYS_SHORT': ['None', 'string'],
                        'UNIT2ROOMS': ['0', 'string'],
                        'UNIT3BATHS': ['0.0', 'string'],
                        'UNIT3BEDS': ['0', 'string'],
                        'UNIT3OWNERTENANTPAYS_SHORT': ['None', 'string'],
                        'UNIT3ROOMS': ['0', 'string'],
                        'UNIT4BATHS': ['0.0', 'string'],
                        'UNIT4BEDS': ['0', 'string'],
                        'UNIT4OWNERTENANTPAYS_SHORT': ['None', 'string'],
                        'UNIT4ROOMS': ['0', 'string'],
                        'UNITSTYLE_SHORT': ['Unknown', 'string'],
                        'USABLE': ['0', 'string'],
                        'UTILITIES_SHORT': ['Unknown', 'string'],
                        'WATERINFO_SHORT': ['Unknown', 'string'],
                        'WATER_SHORT': ['Unknown', 'string'],
                        'WITHDRAWNDATE': ['00/00/0000 00:00:00', 'string'],
                        'YEARBUILT': ['0', 'string'],
                        'ZIPCODE': ['00000', 'string'],
                        'ZONING': ['None', 'string'],
                        'ZONINGDESC_SHORT': ['Unknown', 'string']}

        for col, default_data in datatype_dict.items():
            try:
                df_var[col] = df_var[col].fillna(default_data[0])
                df_var[col] = df_var[col].astype(default_data[1])
                # print(f'{col} dtype changed')

            except ValueError:
                # Fill_na value doesn't meet column datatype
                print(f'Error in type change: {col} - {default_data}')
                pass
            except KeyError:
                # Column doesn't exist in this dataframe
                pass
            except TypeError as e:
                print(f'{e}')
                print(f'Error in type change: {col} - {default_data}')

                raise TypeError

        update_bar.update(1)
        return df_var

    @staticmethod
    def fix_lotsize(string):

        pattern_dict = {'length x width': re.compile(r'(^\d{2,4}(\.\d{0,4})?)\s?X\s?(\d{2,4}(\.\d{0,4})?)', flags=re.IGNORECASE),
                        'square feet': re.compile(r'(^\d{3,6})\s?SF', flags=re.IGNORECASE),
                        'acres': re.compile(r'(\d{0,4}\.?\d{0,5})\s?[ACRES]*?', flags=re.IGNORECASE)
        }

        for description, pattern in pattern_dict.items():
            try:
                if pattern.match(string):
                    found_pattern = pattern.search(string)

                    if description == 'length x width':
                        return float(found_pattern.group(1)) * float(found_pattern.group(3))

                    elif description == 'square feet':
                        return float(found_pattern.group(1))

                    elif description == 'acres':
                        return float(found_pattern.group(1)) * 43560
            except ValueError:
                return 0.0
            except TypeError:
                return 0.0

        return 0.0

    @staticmethod
    def fixer_upper(df_var, prop_type, update_bar):
        """
        REFACTOR
        """

        condition_dict = {
            'RES': ['STYLEPRIMARY_SHORT', 'STYLE_SHORT'],
            'MUL': ['UNITSTYLE_SHORT', 'UNITSTYLE_SHORT']
        }

        temp_df_ = df_var.copy()
        fixup_pattern = re.compile(r'HANDY(\s)?MAN|NEEDS WORK|FIXER(-|\s)?UPPER|BOARDED(\sUP)?'
                                   r'IN NEED OF WORK|NEEDS REHAB|TOTAL REHAB|EXTENSIVE REPAIR|COMPLETE OVERHAUL'
                                   r'YOUR OWN RISK|TLC|INVESTOR SPECIAL|203(\s)?K|PROCEED WITH CAUTION'
                                   r'SIGNIFICANT REPAIR|DAMAGE|CASH(\sOFFER(S)?\s)?ONLY|NEED OF REPAIR|FULL GUT(\sRENOVATION)?'
                                   r'TOTAL GUT(\sRENOVATION)?|(?<!crown\s)(?<!base\s)mold\b(?!ing)', flags=re.IGNORECASE)
        bankowned_pattern = re.compile(r'BANK OWNED|ESTATE SALE|BANK FORECLOSURE|CORPORATE OWNED',flags=re.IGNORECASE)
        short_sale_pattern = re.compile(r'(?<!THIS IS NOT A )(?<!NOT A )SHORT SALE|SUBJECT TO LENDER(S)? APPROVAL'
                                        r'SUBJECT TO THIRD PARTY APPROVAL|SUBJECT TO BANK(S)? APPROVAL', flags=re.IGNORECASE)
        not_short_sale_pattern = re.compile(r'(THIS\sIS\s)?NOT A SHORT SALE', flags=re.IGNORECASE)

        for idx, row in temp_df_.iterrows():

            primary_style = row[condition_dict[prop_type][0]]
            styles = row[condition_dict[prop_type][1]].split(',')

            # Bank Owned pattern
            if bankowned_pattern.search(row['LISTING_REMARKS']) is not None:

                df_var.loc[idx, 'BANK_OWNED'] = True
                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = True
                df_var.loc[idx, 'DISTRESSED_SALE'] = True

            else:
                df_var.loc[idx, 'BANK_OWNED'] = False
                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = False
                df_var.loc[idx, 'DISTRESSED_SALE'] = False

            # Short Sale pattern
            if (short_sale_pattern.search(row['LISTING_REMARKS']) is not None) and (not_short_sale_pattern.search(row['LISTING_REMARKS']) is None):

                df_var.loc[idx, 'SHORT_SALE'] = True
                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = True
                df_var.loc[idx, 'DISTRESSED_SALE'] = True

            else:
                df_var.loc[idx, 'SHORT_SALE'] = False

                if (df_var.loc[idx, 'POTENTIAL_INVESTMENT'] and df_var.loc[idx, 'DISTRESSED_SALE']) != True:
                    df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = False
                    df_var.loc[idx, 'DISTRESSED_SALE'] = False

            # Fixer upper pattern
            if (primary_style == 'FixrUppr') or ('FixrUppr' in styles) or (
                    fixup_pattern.search(row['LISTING_REMARKS']) is not None):

                df_var.loc[idx, 'CONDITION'] = 'Fixer Upper'
                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = True
                df_var.loc[idx, 'DISTRESSED_SALE'] = True

            else:
                df_var.loc[idx, 'CONDITION'] = 'Unknown'

                if (df_var.loc[idx, 'POTENTIAL_INVESTMENT'] and df_var.loc[idx, 'DISTRESSED_SALE']) != True:
                    df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = False
                    df_var.loc[idx, 'DISTRESSED_SALE'] = False

        update_bar.update(1)
        return df_var

    @staticmethod
    def investment_label(df_var, update_bar):

        df_var['INVESTMENT_SALE'] = (df_var['SELLERNAME']
                                         .str.contains('\,?\s?\,?l\s?l\s?c|Investment|Improvement|Builders|Inc\.?|Management|Corp\.?|Group',
                                                       case=False, na=False, regex=True))

        update_bar.update(1)
        return df_var

    @staticmethod
    def load_checkpoint(topic):

        base_path = get_filepath('backups')
        filepath = os.path.join(base_path, f'{topic}.xlsx')

        print(f' ==== PROCESSING ERROR OCCURRED DURING LAST CONSUMPTION ==== ')
        print(f' ==== CONSUMING DATA FROM CHECKPOINT ==== ')
        return pd.read_excel(filepath)

    @staticmethod
    def original_lp_diff(df_var, update_bar):
        try:
            df_var['OLP/LP%'] = round(((df_var['LISTPRICE'] - df_var['ORIGLISTPRICE']) / df_var['ORIGLISTPRICE']) * 100, 0)
            df_var['SP/OLP%'] = round(((df_var['SALESPRICE'] - df_var['ORIGLISTPRICE']) / df_var['ORIGLISTPRICE']) * 100, 0)
        except TypeError:
            df_var['OLP/LP%'] = round(((pd.to_numeric(df_var['LISTPRICE']) - pd.to_numeric(df_var['ORIGLISTPRICE'])) / pd.to_numeric(df_var['ORIGLISTPRICE'])) * 100, 0)
            df_var['SP/OLP%'] = round(((pd.to_numeric(df_var['SALESPRICE']) - pd.to_numeric(df_var['ORIGLISTPRICE'])) / pd.to_numeric(df_var['ORIGLISTPRICE'])) * 100, 0)

        update_bar.update(1)
        return df_var

    @staticmethod
    def parse_property_attr(df_var, prop_type, update_bar):

        attributes_dict = {
            'POOL_SHORT': {'POOL_SHORT': 'Y'},
            'SUBPROPTYPE_SFH': {'SUBPROPTYPE_SFH': 'SinglFam'},
            'FLOORS_SHORT': {'WOOD_FLOORS': 'Wood',
                             'MARBLE_FLOORS': 'Marble',
                             'TILE_FLOORS': 'Tile',
                             'CARPET_FLOORS': 'Carpet',
                             'VINYL_FLOORS': 'Vinyl',
                             'LAMINATE_FLOORS': 'Laminate',
                             'STONE_FLOORS': 'Stone',
                             'PARQUET_FLOORS': 'Parquet'
                             },
            'DRIVEWAYDESC_SHORT': {'OFF_STREET_PKNG': 'OffStret',
                                   '1_CAR_WIDE': '1CarWide',
                                   '2_CAR_WIDE': '2CarWide'},
            'COOLSYSTEM_SHORT': {'WINDOW_AC': 'WindowAC',
                                 'CENTRAL_AC': 'Central',
                                 '1_UNIT_AC': '1Unit',
                                 '2_UNITS_AC': '2Units',
                                 '3_UNITS_AC': '3Units',
                                 'WALL_UNIT_AC': 'WallUnit',
                                 'CEILFAN_AC': 'CeilFan',
                                 'DUCTLESS_AC': 'Ductless',
                                 'MULTIZONE_AC': 'MultiZon'},
            'HEATSRC_SHORT': {'HEAT_SRC_NATGAS': 'GasNatur',
                               'HEAT_SRC_ELECTRIC': 'Electric',
                               'HEAT_SRC_OILABV': 'OilAbIn',
                               'HEAT_SRC_OILBEL': 'OilBelow',
                               'HEAT_SRC_SOLAR': 'SolarLse'},
            'BASEMENT_SHORT': {'BASEMENT_SHORT': 'Y'},
            'BASEDESC_SHORT': {'BASEDESC_BILCOSTY': 'BilcoSty',
                               'BASEDESC_FINISHED': 'Finished',
                               'BASEDESC_FINPART': 'FinPart',
                               'BASEDESC_FRNCHDRN': 'FrnchDrn',
                               'BASEDESC_FULL': 'Full',
                               'BASEDESC_PARTIAL': 'Partial',
                               'BASEDESC_SLAB': 'Slab',
                               'BASEDESC_UNFINISH': 'Unfinish',
                               'BASEDESC_WALKOUT': 'Walkout',
                               'BASEDESC_NONE': 'None'},
            'EXTERIOR_SHORT': {'EXTERIOR_SHORT_DECK': 'Deck',
                               'EXTERIOR_ENCLPRCH': 'EnclPrch',
                               'EXTERIOR_FENCPRIV': 'FencPriv',
                               'EXTERIOR_FENCVNYL': 'FencVnyl',
                               'EXTERIOR_FENCWOOD': 'FencWood',
                               'EXTERIOR_GAZEBO': 'Gazebo',
                               'EXTERIOR_HOTTUB': 'HotTub',
                               'EXTERIOR_METALFNC': 'MetalFnc',
                               'EXTERIOR_OPENPRCH': 'OpenPrch',
                               'EXTERIOR_OUTDRKIT': 'OutDrKit',
                               'EXTERIOR_PATIO': 'Patio',
                               'EXTERIOR_PERGOLA': 'Pergola',
                               'EXTERIOR_SPRINKLR': 'Sprinklr',
                               'EXTERIOR_STORAGE': 'Storage',
                               'EXTERIOR_WORKSHOP': 'Workshop'},
            'ROOF_SHORT': {'ROOF_ASPHSHNG': 'AsphShng',
                           'ROOF_COMPSHNG': 'CompShng',
                           'ROOF_FLAT': 'Flat'},
            'SIDING_SHORT': {'SIDING_ALUMINUM': 'Aluminum',
                             'SIDING_BRICK': 'Brick',
                             'SIDING_CEDARSID': 'CedarSid',
                             'SIDING_CLAPBRD': 'Clapbrd',
                             'SIDING_COMPSHNG': 'CompShng',
                             'SIDING_COMPSIDE': 'CompSide',
                             'SIDING_CONCBRD': 'ConcBrd',
                             'SIDING_METAL': 'Metal',
                             'SIDING_STONE': 'Stone',
                             'SIDING_STUCCO': 'Stucco',
                             'SIDING_VERTICAL': 'Vertical',
                             'SIDING_VINYL': 'Vinyl',
                             'SIDING_WOOD': 'Wood',
                             'SIDING_WOODSHNG': 'WoodShng'},
            'HEATSYSTEM_SHORT': {'HEATSYSTEM_1UNIT': '1Unit',
                                 'HEATSYSTEMT_2UNITS': '2Units',
                                 'HEATSYSTEM_3UNITS': '3Units',
                                 'HEATSYSTEM_4UNITS': '4Units',
                                 'HEATSYSTEM_BSBDCAST': 'BsbdCast',
                                 'HEATSYSTEM_BSBDELEC': 'BsbdElec',
                                 'HEATSYSTEM_BSBDHOTW': 'BsbdHotw',
                                 'HEATSYSTEM_FORCEDHA': 'ForcedHA',
                                 'HEATSYSTEM_MULTIZON': 'MultiZon',
                                 'HEATSYSTEM_RDNTHOTW': 'RdntHotW',
                                 'HEATSYSTEM_RDTRHOTW': 'RdtrHotW',
                                 'HEATSYSTEM_RDTRSTM': 'RdtrStm',
                                 'HEATSYSTEM_REGISTER': 'Register'},
            'SEWER_SHORT': {'SEWER_ASSOCTN': 'Assoctn',
                            'SEWER_PUBLAVAL': 'PublAval',
                            'SEWER_PUBLIC': 'Public',
                            'SEWER_SEPTIC': 'Septic'},
            'WATER_SHORT': {'WATER_ASSOCTN': 'Assoctn',
                            'WATER_PUBLIC': 'Public',
                            'WATER_PRIVATE': 'Private',
                            'WATER_WELL': 'Well',
                            'WATER_WATRXTRA': 'WatrXtra'},
            'UTILITIES_SHORT': {'UTILITIES_ALLUNDER': 'AllUnder',
                                'UTILITIES_ELECTRIC': 'Electric',
                                'UTILITIES_GASNATUR': 'GasNatur',
                                'UTILITIES_GASINSTR': 'GasInStr',
                                'UTILITIES_GASPROPN': 'GasPropn'},
            'EASEMENT_SHORT': {'EASEMENT_SHORT': 'Y'},
            'UNITSTYLE_SHORT': {'UNITSTYLE_ONESTORY': 'OneStory',
                                'UNITSTYLE_TWOSTORY': 'TwoStory',
                                'UNITSTYLE_THREESTORY': 'ThreStry',
                                'UNITSTYLE_DUPLEX': 'Duplex',
                                'UNITSTYLE_TRIPLEX': 'Triplex',
                                'UNITSTYLE_FOURPLEX': 'FourPlex',
                                'UNITSTYLE_UNDROVER': 'UndrOver'},
            'BUILDINGSINCLUDED_SHORT': {'BUILDINGSINCLUDED_BARN': 'Barn',
                                        'BUILDINGSINCLUDED_BLDGRMVD': 'BldgRmvd',
                                        'BUILDINGSINCLUDED_GARAGE': 'Garage',
                                        'BUILDINGSINCLUDED_NOBLDGS': 'NoBldgs',
                                        'BUILDINGSINCLUDED_NOVALUE': 'NoValue',
                                        'BUILDINGSINCLUDED_RESIDENC': 'Residenc',
                                        'BUILDINGSINCLUDED_TENOCCUP': 'TenOccup',
                                        'BUILDINGSINCLUDED_UTILBLDG': 'UtilBldg',
                                        'BUILDINGSINCLUDED_WELLMNTD': 'WellMntd'},
            'CURRENTUSE_SHORT': {'CURRENTUSE_COMMERCL': 'Commercl',
                                 'CURRENTUSE_FARMHORS': 'FarmHors',
                                 'CURRENTUSE_FARMORCH': 'FarmOrch',
                                 'CURRENTUSE_INDUSTRL': 'Industrl',
                                 'CURRENTUSE_RESIDENT': 'Resident',
                                 'CURRENTUSE_VCNTIMPR': 'VcntImpr',
                                 'CURRENTUSE_VCNTUNIM': 'VcntUnim',
                                 'CURRENTUSE_WETLANDS': 'WetLands'},
            'DEVSTATUS_SHORT': {'DEVSTATUS_BLDGPERM': 'BldgPerm',
                                'DEVSTATUS_FINSHLOT': 'FinshLot',
                                'DEVSTATUS_PLANAPPD': 'PlanAppd',
                                'DEVSTATUS_PLANFILD': 'PlanFild',
                                'DEVSTATUS_PLANREQD': 'PlanReqd',
                                'DEVSTATUS_RAWLAND': 'RawLand',
                                'DEVSTATUS_ROUGHGRD': 'RoughGrd',
                                'DEVSTATUS_SUBBUYXP': 'SubBuyXp',
                                'DEVSTATUS_SUBFINAP': 'SubFinAp',
                                'DEVSTATUS_SUBPREAP': 'SubPreAp',
                                'DEVSTATUS_VARAPPRD': 'VarApprd',
                                'DEVSTATUS_VARBYOBT': 'VarByObt',
                                'DEVSTATUS_VARREQRD': 'VarReqrd'},
            'IMPROVEMENTS_SHORT': {'IMPROVEMENTS_CURBS': 'Curbs',
                                   'IMPROVEMENTS_FENCE': 'Fence',
                                   'IMPROVEMENTS_FILLED': 'Filled',
                                   'IMPROVEMENTS_NONE': 'None',
                                   'IMPROVEMENTS_NOPUB': 'NoPub',
                                   'IMPROVEMENTS_SHADTREE': 'ShadTree',
                                   'IMPROVEMENTS_SIDEWALK': 'SideWalk',
                                   'IMPROVEMENTS_UTILINST': 'UtilInSt',
                                   'IMPROVEMENTS_UTILONPR': 'UtilOnPr'},
            'LOTDESC_SHORT': {'LOTDESC_CORNER': 'Corner',
                              'LOTDESC_CULDESAC': 'CulDeSac',
                              'LOTDESC_FLAGLOT': 'Flaglot',
                              'LOTDESC_IRREGULR': 'Irregulr',
                              'LOTDESC_LAKEFRNT': 'LakeFrnt',
                              'LOTDESC_LAKONLOT': 'LakOnLot',
                              'LOTDESC_LEVEL': 'Level',
                              'LOTDESC_LKWTVIEW': 'LkWtView',
                              'LOTDESC_MTNVIEW': 'MtnView',
                              'LOTDESC_OPEN': 'Open',
                              'LOTDESC_POND': 'Pond',
                              'LOTDESC_POSSSUBD': 'PossSubd',
                              'LOTDESC_PRIVATE': 'Private',
                              'LOTDESC_SKYLVIEW': 'SkyLView',
                              'LOTDESC_STREAM': 'Stream',
                              'LOTDESC_WATRFRNT': 'WatrFrnt',
                              'LOTDESC_WOODED': 'Wooded'},
            'ROADSURFACEDESC_SHORT': {'ROADSURFACEDESC_BLACKTOP': 'Blacktop',
                                      'ROADSURFACEDESC_CONCRETE': 'Concrete',
                                      'ROADSURFACEDESC_CRUSHSTN': 'CrushStn',
                                      'ROADSURFACEDESC_DIRT': 'Dirt',
                                      'ROADSURFACEDESC_GRAVEL': 'Gravel',
                                      'ROADSURFACEDESC_PRIVATE': 'Private',
                                      'ROADSURFACEDESC_PUBLIC': 'Public'},
            'SEWERINFO_SHORT': {'SEWERINFO_500-': '500-',
                                'SEWERINFO_500-1000': '500-1000',
                                'SEWERINFO_BUYPYHUP': 'BuyPyHUp',
                                'SEWERINFO_CESSPOOL': 'Cesspool',
                                'SEWERINFO_INTAX': 'InTax',
                                'SEWERINFO_NONE': 'None',
                                'SEWERINFO_PUBLINST': 'PublInSt',
                                'SEWERINFO_PUBLONPR': 'PublOnPr',
                                'SEWERINFO_SPTONSIT': 'SptOnSit',
                                'SEWERINFO_SPTSYREQ': 'SptSyReq',
                                'SEWERINFO_STRMSEWR': 'StrmSewr'},
            'SITEPARTICULARS_SHORT': {'SITEPARTICULARS_CLRDALL': 'ClrdAll',
                                      'SITEPARTICULARS_CLRDPART': 'ClrdPart',
                                      'SITEPARTICULARS_LANDFILL': 'LandFill',
                                      'SITEPARTICULARS_LIGHTING': 'Lighting',
                                      'SITEPARTICULARS_MOWDGRAS': 'MowdGras',
                                      'SITEPARTICULARS_PASTORAL': 'Pastoral',
                                      'SITEPARTICULARS_SOMEFLD': 'SomeFld',
                                      'SITEPARTICULARS_STRLKPND': 'StrLkPnd',
                                      'SITEPARTICULARS_STRMDRAN': 'StrmDran',
                                      'SITEPARTICULARS_VIEW': 'View',
                                      'SITEPARTICULARS_WATRACCS': 'WatrAccs',
                                      'SITEPARTICULARS_WATRFRNT': 'WatrFrnt',
                                      'SITEPARTICULARS_WATRVIEW': 'WatrView',
                                      'SITEPARTICULARS_WETLNONE': 'WetlNone',
                                      'SITEPARTICULARS_WETLSOME': 'WetlSome'},
            'WATERINFO_SHORT': {'WATERINFO_1000+': '1000+',
                                'WATERINFO_500': '500',
                                'WATERINFO_BUYPYHUP': 'BuyPyHUp',
                                'WATERINFO_NONE': 'None',
                                'WATERINFO_PUBLINST': 'PublInSt',
                                'WATERINFO_PUBLONPR': 'PublOnPr',
                                'WATERINFO_SEEREM': 'SeeRem',
                                'WATERINFO_WELLREQD': 'WellReqd',
                                'WATERINFO_WLLONSIT': 'WllOnSit'},
            'ZONINGDESC_SHORT': {'ZONINGDESC_BUSNDIST': 'BusnDist',
                                 'ZONINGDESC_CONDO': 'Condo',
                                 'ZONINGDESC_FLOODWET': 'FloodWet',
                                 'ZONINGDESC_GENCOMMR': 'GenCommr',
                                 'ZONINGDESC_HIWYCOMM': 'HiwyComm',
                                 'ZONINGDESC_LIGHTIND': 'LightInd',
                                 'ZONINGDESC_LOWINCOM': 'LowIncom',
                                 'ZONINGDESC_MULTIFAM': 'MultiFam',
                                 'ZONINGDESC_OFCRSRCH': 'OfcRsrch',
                                 'ZONINGDESC_ONEFAMLY': 'OneFamly',
                                 'ZONINGDESC_PLANRES': 'PlanRes',
                                 'ZONINGDESC_SEEREM': 'SeeRem',
                                 'ZONINGDESC_SHOPCNTR': 'ShopCntr',
                                 'ZONINGDESC_TWOFAMLY': 'TwoFamly'},
            'PETS_SHORT': {'PETS_SHORT': '^[^N]'},
            'RENTINCLUDES_SHORT': {'RENTINCLUDES_BLDGINSR': 'BldgInsr',
                                   'RENTINCLUDES_COOLING': 'Cooling',
                                   'RENTINCLUDES_ELECTRIC': 'Electric',
                                   'RENTINCLUDES_GAS': 'Gas',
                                   'RENTINCLUDES_HEAT': 'Heat',
                                   'RENTINCLUDES_JANITSRV': 'JanitSrv',
                                   'RENTINCLUDES_MAINTBLG': 'MaintBlg',
                                   'RENTINCLUDES_MAINTCOM': 'MaintCom',
                                   'RENTINCLUDES_SEWER': 'Sewer',
                                   'RENTINCLUDES_TAXES': 'Taxes',
                                   'RENTINCLUDES_TRASHREM': 'TrashRem',
                                   'RENTINCLUDES_WATER': 'Water'},
            'TENANTPAYS_SHORT': {'TENANTPAYS_CABLE': 'Cable',
                                 'TENANTPAYS_ELECTRIC': 'Electric',
                                 'TENANTPAYS_GAS': 'Gas',
                                 'TENANTPAYS_HEAT': 'Heat',
                                 'TENANTPAYS_HOTWATER': 'HotWater',
                                 'TENANTPAYS_SEWER': 'Sewer',
                                 'TENANTPAYS_SNOWREMV': 'SnowRemv',
                                 'TENANTPAYS_TENPYREP': 'TenPyRep',
                                 'TENANTPAYS_TRASHREM': 'TrashRem',
                                 'TENANTPAYS_WATER': 'Water'}
        }

        try:
            target_attr = {
                'RES': zip(list(attributes_dict.keys())[:16], list(attributes_dict.values())[:16]),
                'MUL': zip(list(attributes_dict.keys())[3:17], list(attributes_dict.values())[3:17]),
                'LND': zip(list(attributes_dict.keys())[17:27], list(attributes_dict.values())[17:27]),
                'RNT': zip(list(attributes_dict.keys())[2:8] + list(attributes_dict.keys())[11:14] + (list(attributes_dict.keys())[27:]),
                           list(attributes_dict.values())[2:14] + list(attributes_dict.values())[11:14] + list(attributes_dict.values())[27:]),
            }

            for target_col, values in target_attr[prop_type]:

                for new_col, pattern in values.items():

                    df_var[new_col] = df_var[target_col].str.contains(pattern, case=True, na=False, regex=True)

        except KeyError:
            pass

        update_bar.update(1)
        return df_var

    @staticmethod
    def parse_month(value):

        return int(str(value).split('/')[0])

    @staticmethod
    def parse_year(value):

        return int(str(value).split('/')[2][:4])

    def prepare_data(self, df_list, prop_type, wait_time=0, retries=0):
        """
        REFACTOR
        """

        df = pd.concat(df_list)

        if prop_type in ['RES', 'MUL', 'LND']:
            # Check the column dtype. Will throw error if not datetime64[ns]
            scrape_date_column_datatype = df["SCRAPED_DATE"].dtype

            if not isinstance(scrape_date_column_datatype, numpy.dtypes.DateTime64DType):
                df["SCRAPED_DATE"] = pd.to_datetime(df["SCRAPED_DATE"])

            mask = df["SCRAPED_DATE"] >= pd.Timestamp(datetime.now().date() - timedelta(days=3))

        print(f' ==== PREPARING NEW DATA. COMMITTING CONSUMPTION ====')

        try:
            if prop_type in ['RES', 'MUL', 'LND']:
                # Used for RES, MUL, LND property types
                self.consumer.commit()
                df = df[mask]
                return df.drop_duplicates(subset=['STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'LISTDATE'],
                                          keep='last').reset_index(drop=True)

            elif prop_type == 'RNT':
                try:
                    # Used for RNT proeprty types
                    self.consumer.commit()
                    return df.drop_duplicates(subset=['STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'RENTEDDATE'],
                                              keep='last').reset_index(drop=True)
                except KeyError:
                    df.insert(17, 'RENTEDDATE', '00/00/0000 00:00:00')
                    return df.drop_duplicates(subset=['STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'RENTEDDATE'],
                                              keep='last').reset_index(drop=True)

            elif prop_type == 'TAX':
                # Used for TAX data
                self.consumer.commit()
                return df.drop_duplicates(subset=['AUTOROW'], keep='last').reset_index(drop=True)

            else:
                # Used for property images
                self.consumer.commit()
                return df.drop_duplicates(subset=['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', ],
                                          keep='last').reset_index(drop=True)

        except RebalanceInProgressError:
            print(f' ==== BROKER REBALANCING IN PROGRESS. RETRYING EFFORT ==== ')

            assert retries <= 10, " ==== BROKER REBALANCING OVER 50 MINUTES, ENDING PROGRAM ==== "
            time.sleep(wait_time)
            self.consumer.poll()
            self.prepare_data(df_list, prop_type, wait_time=300, retries=retries + 1)

    @staticmethod
    def process_keys(keys_list):

        municipality_data = {
            "Counties": {},
            "prop_type": "",
            "year": ""
        }

        for item in keys_list:

            item_list = item.split(" ")
            county = item_list[-5]
            municipality = " ".join(item_list[:-5])

            if municipality_data["prop_type"] == "":
                municipality_data["prop_type"] = item_list[-3]

            if municipality_data["year"] == "":
                municipality_data["year"] = item_list[-4][-4:]

            if county not in list(municipality_data["Counties"].keys()):
                municipality_data["Counties"].setdefault(county, [])

            if municipality not in municipality_data["Counties"][county]:
                municipality_data["Counties"][county].append(municipality)

        for key, value in municipality_data["Counties"].items():
            # Need to figure out how to properly display counties with more than one word
            print(f" ==== SUCCESSFULLY STORED {key} COUNTY {municipality_data['prop_type']} PROPERTY DATA IN "
                  f"{municipality_data['year']} FOR THE FOLLOWING MUNICIPALITIES: ==== \n"
                  f" ==== {value}")

    def produce_images(self, df_var, prop_type):
        """
        REFACTOR
        """

        if prop_type in ['RES', 'MUL', 'RNT']:
            if prop_type == 'RES':
                image_df = df_var[['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                                   'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'STYLEPRIMARY_SHORT',
                                   'CONDITION', 'LISTDATE', 'IMAGES', 'PROP_CLASS', 'LATITUDE', 'LONGITUDE',
                                   'LISTING_REMARKS', 'SCRAPED_DATE', 'SALESPRICE']]
            elif prop_type == 'MUL':
                image_df = df_var[['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                                    'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'UNITSTYLE_SHORT',
                                    'CONDITION', 'LISTDATE', 'IMAGES', 'PROP_CLASS', 'LATITUDE', 'LONGITUDE',
                                   'LISTING_REMARKS', 'SCRAPED_DATE', 'SALESPRICE']]

            elif prop_type == 'RNT':
                image_df = df_var[['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
                                    'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'CONDITION',
                                    'RENTEDDATE', 'PROPERTYTYPEPRIMARY_SHORT', 'PROPSUBTYPERN',
                                   'IMAGES', 'PROP_CLASS', 'LATITUDE', 'LONGITUDE',
                                   'LISTING_REMARKS', 'SCRAPED_DATE', 'RENTMONTHPERLSE']]
            # elif prop_type == 'LND':
            #     image_df = df_var[['MLSNUM', 'STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'COUNTY', 'ZIPCODE',
            #                         'TOWNCODE', 'COUNTYCODE', 'BLOCKID', 'LOTID', 'TAXID', 'CONDITION',
            #                         'LISTDATE', 'CURRENTUSE_SHORT', 'DEVSTATUS_SHORT', 'LOTDESC_SHORT',
            #                        'IMAGES', 'PROP_CLASS', 'LATITUDE', 'LONGITUDE',
            #                        'LISTING_REMARKS', 'SCRAPED_DATE', 'SALESPRICE']]

            prepared_image_df = image_df.to_json(orient='split', date_format='iso')
            try:
                results = self.producer.send('prop_images', value=prepared_image_df)
                result_metadata = results.get(timeout=10)

            except MessageSizeTooLargeError:
                self.reduce_df_size(image_df, 500)

            except KafkaTimeoutError:
                print(f' === IMAGES HAVE NOT BEEN PRODUCED TO {result_metadata.topic} IN KAFKA')

            print(f" ==== SUCCESSFULLY PRODUCED {prop_type} PROPERTY IMAGES TO KAFKA ==== ")

    @staticmethod
    def reorder_columns(df_var, prop_type, update_bar):

        # Create a way to dynamically change the columns to reorder based on property type
        cols_list = df_var.columns
        cols_index = df_var.columns.get_indexer(cols_list)

        location_dict = {'RES': {'MLS': 1,
                            'LATITUDE': 13,
                            'LONGITUDE': 14,
                            'CONDITION': 16,
                            'OLP/LP%': 19,
                            'SP/OLP%': 22,
                            'INVESTMENT_SALE': 23,
                            'POTENTIAL_INVESTMENT': 24,
                            'DISTRESSED_SALE': 25,
                            'SHORT_SALE': 26,
                            'BANK_OWNED': 27,
                            'BATHSFULLTOTAL': 31,
                            'BATHSHALFTOTAL': 32,
                            'SQFTAPPROX': 34,
                            'LOTSIZE': 35,
                            'LOTSIZE (SQFT)': 36,
                            'ASSESSAMOUNTBLDG': 37,
                            'ASSESSAMOUNTLAND': 38,
                            'ASSESSTOTAL': 39,
                            'TAXAMOUNT': 40,
                            'QTR': 44,
                            'MONTH': 45,
                            'YEAR': 46,
                            'DAYS_TO_CLOSE': 51,
                            'ANTIC_CLOSEDATE_DIFF': 52,
                            'LISTING_REMARKS': df_var.shape[1] - 1,
                            'PYSPARK_PROCESSED': df_var.shape[1] - 1,
                            'SCRAPED_DATE': df_var.shape[1] - 1},
                         'MUL': {'MLS': 1,
                            'LATITUDE': 13,
                            'LONGITUDE': 14,
                            'CONDITION': 16,
                            'OLP/LP%': 19,
                            'SP/OLP%': 22,
                            'INVESTMENT_SALE': 23,
                            'POTENTIAL_INVESTMENT': 24,
                            'DISTRESSED_SALE': 25,
                            'SHORT_SALE': 26,
                            'BANK_OWNED': 27,
                            'SQFTBLDG': 35,
                            'LOTSIZE': 36,
                            'LOTSIZE (SQFT)': 37,
                            'ASSESSAMOUNTBLDG': 38,
                            'ASSESSAMOUNTLAND': 39,
                            'ASSESSTOTAL': 40,
                            'TAXAMOUNT': 42,
                            'QTR': 47,
                            'MONTH': 48,
                            'YEAR': 49,
                            'DAYS_TO_CLOSE': 50,
                            'ANTIC_CLOSEDATE_DIFF': 51,
                            'LISTING_REMARKS': df_var.shape[1] - 1,
                            'PYSPARK_PROCESSED': df_var.shape[1] - 1,
                            'SCRAPED_DATE': df_var.shape[1] - 1},
                         'LND': {'MLS': 1,
                            'LATITUDE': 13,
                            'LONGITUDE': 14,
                            'CONDITION': 16,
                            'OLP/LP%': 19,
                            'SP/OLP%': 22,
                            'INVESTMENT_SALE': 23,
                            'LOTSIZE': 24,
                            'LOTSIZE (SQFT)': 25,
                            'ASSESSAMOUNTBLDG': 26,
                            'ASSESSAMOUNTLAND': 27,
                            'ASSESSTOTAL': 28,
                            'TAXAMOUNT': 29,
                            'QTR': 30,
                            'MONTH': 31,
                            'YEAR': 32,
                            'DAYS_TO_CLOSE': 33,
                            'ANTIC_CLOSEDATE_DIFF': 34,
                            'LISTING_REMARKS': df_var.shape[1] - 1,
                            'PYSPARK_PROCESSED': df_var.shape[1] - 1,
                            'SCRAPED_DATE': df_var.shape[1] - 1},
                         'RNT': {'MLS': 1,
                            'LATITUDE': 13,
                            'LONGITUDE': 14,
                            'LISTING_REMARKS': df_var.shape[1] - 1,
                            'PYSPARK_PROCESSED': df_var.shape[1] - 1,
                            'SCRAPED_DATE': df_var.shape[1] - 1},
                         'TAX': {
                             'DEDATE': KafkaGSMLSConsumer.column_indexer('DATEMODIFIED',
                                                                                    cols_list, cols_index) + 1,
                             'LASTMODDATE': KafkaGSMLSConsumer.column_indexer('GSCOUNTYCODE',
                                                                         cols_list, cols_index) + 2,
                             'LOCNUM_NUM': KafkaGSMLSConsumer.column_indexer('LOCNUM',
                                                                              cols_list, cols_index) + 3,
                             'PREVOWN_POSS_TIME': KafkaGSMLSConsumer.column_indexer('PRIORSALEDATE',
                                                                                    cols_list, cols_index) + 4,
                             'ROWID': KafkaGSMLSConsumer.column_indexer('RECDATE',
                                                                             cols_list, cols_index) + 5}
                         }
        if location_dict[prop_type] != {}:
            for col, value in location_dict[prop_type].items():
                df_var.insert(value, col, df_var.pop(col))

        update_bar.update(1)
        return df_var

    @staticmethod
    def res_property_cleaning(df_var, prop_type, update_bar):

        return (df_var.pipe(KafkaGSMLSConsumer.fill_na_values, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.standard_cleaning, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.convert_lot_size, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.calculate_dates, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.change_datatypes, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.combine_listing_remarks, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.sub_property_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.parse_property_attr, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.investment_label, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.fixer_upper, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.original_lp_diff, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.reorder_columns, prop_type=prop_type, update_bar=update_bar)
                    .pipe(KafkaGSMLSConsumer.escape_illegal_char, prop_type=prop_type, update_bar=update_bar))

    @staticmethod
    def mul_property_cleaning(df_var, prop_type, update_bar):

        return (df_var.pipe(KafkaGSMLSConsumer.fill_na_values, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.standard_cleaning, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.convert_lot_size, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.calculate_dates, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.change_datatypes, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.combine_listing_remarks, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.parse_property_attr, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.investment_label, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.fixer_upper, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.original_lp_diff, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.reorder_columns, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.escape_illegal_char, prop_type=prop_type, update_bar=update_bar))

    @staticmethod
    def lnd_property_cleaning(df_var, prop_type, update_bar):

        return (df_var.pipe(KafkaGSMLSConsumer.fill_na_values, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.standard_cleaning, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.convert_lot_size, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.calculate_dates, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.change_datatypes, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.combine_listing_remarks, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.parse_property_attr, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.investment_label, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.original_lp_diff, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.reorder_columns, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.escape_illegal_char, prop_type=prop_type, update_bar=update_bar))

    @staticmethod
    def rnt_property_cleaning(df_var, prop_type, update_bar):

        return (df_var.pipe(KafkaGSMLSConsumer.fill_na_values, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.standard_cleaning, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.calculate_dates, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.change_datatypes, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.combine_listing_remarks, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.parse_property_attr, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.reorder_columns, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.escape_illegal_char, prop_type=prop_type, update_bar=update_bar))

    def reduce_df_size(self, df_var, step: int, block_num=None):

        for idx, i in enumerate(range(0, len(df_var), step)):
            slice_df = df_var[i:i + step]

            prepared_image_df = slice_df.to_json(orient='split', date_format='iso')
            try:
                results = self.producer.send('prop_images', value=prepared_image_df)
                result_metadata = results.get(timeout=10)
                if block_num is None:
                    print(f'Image data produced to Kafka: Block {idx}')
                else:
                    print(f'Image data produced to Kafka: Sub-Block {idx} of Block {block_num}')
            except MessageSizeTooLargeError:
                self.reduce_df_size(slice_df, step // 50, idx)

            except KafkaTimeoutError:
                print(f'Images have not been produced to {result_metadata.topic} in Kafka')

    @staticmethod
    def tax_property_cleaning(df_var, prop_type, update_bar):

        return (df_var.pipe(KafkaGSMLSConsumer.fill_na_values, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.standard_cleaning, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.calculate_dates, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.change_datatypes, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.reorder_columns, prop_type=prop_type, update_bar=update_bar)
                .pipe(KafkaGSMLSConsumer.escape_illegal_char, prop_type=prop_type, update_bar=update_bar))

    @staticmethod
    def standard_cleaning(df_var, prop_type, update_bar):

        cleaning_dict = {
            'ALL': {'*': {'columns': ['ACRES', 'BLOCKID', 'COUNTY', 'COUNTYCODE', 'LOTID', 'LOTSIZE',
                                      'OWNERNAME', 'STREETNAME', 'SQFTBLDG', 'TAXID', 'TOWNCODE', 'ZIPCODE'],
                          'default_value': '',
                          'regex': False},
                    '%': {'columns': ['SP/LP%'],
                          'default_value': '',
                          'regex': False},
                    '\.?\*?\(\d{4}\*?\)': {'columns': ['TOWN'],
                                           'default_value': '',
                                           'regex': True}
                    },
            'RES': {'00:00:00': {'columns': ['ASSESSAMOUNTBLDG', 'ASSESSTOTAL', 'ASSESSAMOUNTLAND', 'APPFEE'],
                                 'default_value': '0.0',
                                 'regex': False},
                    '^Assoctn(,\w+)?': {'columns': ['WATER_SHORT'],
                                        'default_value': 'Assoctn',
                                        'regex': True},
                    '^Private(,\w+)?': {'columns': ['WATER_SHORT'],
                                        'default_value': 'Private',
                                        'regex': True},
                    '^Public(,\w+)?': {'columns': ['WATER_SHORT'],
                                        'default_value': 'Public',
                                        'regex': True},
                    '^Well(,\w+)?': {'columns': ['WATER_SHORT'],
                                        'default_value': 'Well',
                                        'regex': True},
                    '^WatrXtra(,\w+)?': {'columns': ['WATER_SHORT'],
                                        'default_value': 'WatrXtra',
                                        'regex': True},
                    '(\d)\1{3,}': {'columns': ['SQFTAPPROX', 'YEARBUILT'],
                                        'default_value': '0.0',
                                        'regex': True}
                    },
            'MUL': {'00:00:00': {'columns': ['ASSESSAMOUNTBLDG', 'ASSESSTOTAL', 'ASSESSAMOUNTLAND'],
                                 'default_value': '0.0',
                                 'regex': False},
                    '(\d)\1{3,}': {'columns': ['YEARBUILT', 'SQFTBLDG', 'INCOMENETOPERATING', 'EXPENSEOPERATING', 'INCOMEGROSSOPERATING'],
                                   'default_value': '0.0',
                                   'regex': True}
                    },
            'LND': {'00:00:00': {'columns': ['ASSESSAMOUNTBLDG', 'ASSESSTOTAL', 'ASSESSAMOUNTLAND'],
                                 'default_value': '0.0',
                                 'regex': False}
                    },
            'RNT': {'*': {'columns': ['BLOCKID', 'COUNTY', 'COUNTYCODE', 'LOTID',
                                      'STREETNAME', 'TAXID', 'TOWNCODE', 'ZIPCODE'],
                          'default_value': '',
                          'regex': False},
                    '%': {'columns': ['RP/LP%'],
                          'default_value': '',
                          'regex': False},
                    '\.?\*?\(\d{4}\*?\)': {'columns': ['TOWN'],
                                           'default_value': '',
                                           'regex': True}},
            'TAX': {'.0': {'columns': ['ASSESSMENT1', 'ASSESSMENT2', 'ASSESSMENT3', 'TOTALASSESSMENT', 'YEARBUILT'],
                           'default_value': '', 'regex': False}}
        }

        if prop_type in ['RES', 'MUL', 'LND']:
            cleaning_list = [cleaning_dict['ALL'], cleaning_dict[prop_type]]
        elif prop_type == 'RNT' or prop_type == 'TAX':
            cleaning_list = [cleaning_dict[prop_type]]

        for cleaning_group in cleaning_list:

            for char, collection in cleaning_group.items():

                if char is not None:
                    for col in collection['columns']:
                        try:
                            df_var[col] = df_var[col].str.replace(char, collection['default_value'],
                                                                  regex=collection['regex'])
                        except AttributeError as e:
                            print(f' ==== ERROR AT {col}: {e} ==== ')
                            raise AttributeError

                        except KeyError as e:
                            print(f' ==== COLUMN {col} DOES NOT EXISTS FOR {prop_type} DATA ==== ')

        df_var = KafkaGSMLSConsumer.baths_empty(df_var, prop_type)

        update_bar.update(1)
        return df_var

    @staticmethod
    def sub_property_type(df_var, update_bar):
        """
        REFACTOR
        """

        temp_df_ = df_var.copy()
        target_styles = ['TwnIntUn', 'OneFloor', 'MultiFlr', 'TwnEndUn', 'FirstFlr', 'HighRise']

        for idx, row in temp_df_.iterrows():

            boolean_list = [True if i in target_styles else False for i in row['STYLE_SHORT'].split(',')]

            if row['STYLEPRIMARY_SHORT'] in  target_styles:

                if row['SUBPROPTYPE_SFH'] == 'CCT':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE_SFH'] = 'CCT'

            elif row['STYLEPRIMARY_SHORT'] not in target_styles and True in boolean_list:

                if row['SUBPROPTYPE_SFH'] == 'SinglFam':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE_SFH'] = 'SinglFam'

            else:
                if row['SUBPROPTYPE_SFH'] == 'SinglFam':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE_SFH'] = 'SinglFam'

        update_bar.update(1)
        return df_var

    def submit2sql(self, df_var, topic, prop_type, cleaning_bar):

        step = 500

        for idx, row in enumerate(range(0, len(df_var), step)):

            slice_df = df_var[row:row + step]
            final_df = slice_df.pipe(KafkaGSMLSConsumer.drop_columns,
                                     prop_type=prop_type, update_bar=cleaning_bar)

            try:
                # Employs drop column for the following property data types
                # print(f'{final_df.info()}')
                final_df.to_sql(topic, con=self.connection, if_exists='append', index=False)

            # Catches data which is invalid for specific column or breaks the unique key constraints
            except (DataError, IntegrityError) as e:
                print(f'{e}')
                print(f' ==== ERROR HAS BEEN DETECTED IN BLOCK {idx}. NOW SUBMITTING DATA BY INDIVIDUAL ROW ==== ')
                self.submit2sql_dataerror(final_df, topic)

        print(f" ==== {topic} HAS SUCCESSFULLY BEEN STORED IN POSTGRESQL ==== ")

    def submit2sql_dataerror(self, df_var, topic):
        """
        Submits data row by row in the event of  SQL error being thrown
        during the submit2sql function.

        :param df_var:
        :param topic:
        """

        for idx, row in df_var.iterrows():

            temp_df = pd.DataFrame(data=row.values.reshape(1, -1), index=[idx], columns=row.index)

            try:
                temp_df.to_sql(topic, con=self.connection, if_exists='append', index=False)
            except (DataError, IntegrityError) as e:
                print(f'Faulty row has been found at Row {idx}.\nError has occurred: {e}')
                continue

    def submit_data(self, df, prop, table, cleaning_bar, keys_list, re_image_obj, **kwargs):

        # filepath = f"/opt/airflow/downloads/test_data_{datetime.now().date()}.xlsx"

        if prop != 'IMAGES':
            # Produce image data to Kafka topic and relational data to SQL
            if prop in ['RES', 'MUL', 'RNT']:
                print(' ==== IMAGES PRODUCED ==== ')
                self.produce_images(df, prop)

            self.submit2sql(df, table, prop, cleaning_bar)
            if keys_list is not None:
                KafkaGSMLSConsumer.process_keys(keys_list)  # Print the data that has been processed

        else:
            # Produce image data to MongoDB
            if re_image_obj is not None:
                re_image_obj.main(df, **kwargs)
                print(f" ==== {table.upper()} IMAGES HAVE SUCCESSFULLY BEEN STORED IN MONGODB ====")
            else:
                raise TypeError(" ==== REALESTATEIMAGES CLASS WAS NOT INITIATED. IMAGES NOT STORED IN MONGODB ==== ")

    @logger_decorator
    def main(self, prop, retry=False, **kwargs):

        logger = kwargs['logger']
        topic_data = self.prop_dict[prop]
        cleaning_bar = tqdm(total=topic_data['functions'], desc='Cleaning Functions', colour='blue')
        re_image_obj = None
        if prop == 'IMAGES':
            re_image_obj = RealEstateImages(latest_order_num=64872924)
        self.consumer.subscribe([topic_data['topic']])
        # Using the subscribe method instead of directly assigning a topic so Kafka can
        # handle the re-balancing of partitions for me

        try:
            while True:

                if retry is False:
                    print(f' ==== CONSUMING {prop} DATA FROM KAFKA ==== ')
                    temp_df, associate_keys = self.consume_data(prop)

                else:
                    temp_df = KafkaGSMLSConsumer.load_checkpoint(topic_data['topic'])
                    associate_keys = None
                    retry = False
                    KafkaGSMLSConsumer.delete_checkpoint(topic_data['topic'])

                if isinstance(temp_df, pd.DataFrame):
                    final_df = KafkaGSMLSConsumer.create_final_df(temp_df, prop,
                                                                  topic_data['clean_type'], cleaning_bar)
                    self.submit_data(final_df, prop, topic_data['topic'], cleaning_bar, associate_keys, re_image_obj)
                else:
                    break

            self.consumer.close()
            return True

        except TypeError as e:
            logger.warning(f'{e}')
            logger.warning(f'{traceback.print_exception(e)}')
            KafkaGSMLSConsumer.checkpoint(temp_df, topic_data['topic'])
            raise TypeError

        except AssertionError as e:
            print(" ==== BROKER REBALANCING OVER 50 MINUTES, ENDING PROGRAM ==== ")
            return e

        except (DatabaseError, SyntaxError) as e:
            print(f" ==== A SQL DATABASE ERROR HAS OCCURRED ==== \n{e}")
            KafkaGSMLSConsumer.checkpoint(temp_df, topic_data['topic'])
            return e

