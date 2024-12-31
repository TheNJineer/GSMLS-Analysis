import os

from kafka import KafkaConsumer
from io import StringIO
import json
from pprint import pprint
from kafka.errors import KafkaTimeoutError
import pandas as pd
from tabulate import tabulate
import re

class KafkaGSMLSConsumer:

    @staticmethod
    def calculate_dates(df_var):

        df_var['CLOSEDDATE'] = pd.to_datetime(df_var['CLOSEDDATE'], errors='coerce')
        df_var['PENDINGDATE'] = pd.to_datetime(df_var['PENDINGDATE'], errors='coerce')
        df_var['ANTICCLOSEDDATE'] = pd.to_datetime(df_var['ANTICCLOSEDDATE'], errors='coerce')
        df_var['DAYS_TO_CLOSE'] = df_var['CLOSEDDATE'] - df_var['PENDINGDATE']
        df_var['ANTIC_CLOSEDATE_DIFF'] = df_var['CLOSEDDATE'] - df_var['ANTICCLOSEDDATE']

        return df_var

    @staticmethod
    def combine_listing_remarks(df_var):

        df_var['LISTING_REMARKS'] = (df_var['REMARKSPUBLIC']
                                     .str.cat([df_var['REMARKSAGENT'], df_var['SHOWSPECIAL']], na_rep='_', sep='. '))

        return df_var

    @staticmethod
    def convert_lot_size(df_var):

        df_var['ACRES'] = df_var['ACRES'].astype('float64')
        df_var['LOTSIZE (SQFT)'] = df_var['ACRES'] * 43560

        temp_df_ = df_var.copy()

        for idx, row in temp_df_.iterrows():

            if row['LOTSIZE (SQFT)'] == 0.0:
                value = row['LOTSIZE']
                df_var.loc[idx, 'LOTSIZE (SQFT)'] = KafkaGSMLSConsumer.fix_lotsize(value)

        return df_var

    @staticmethod
    def drop_columns(df_var):

        return df_var.drop(columns=['ACRES', 'REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'DRIVEWAYDESC_SHORT',
									 'COOLSYSTEM_SHORT', 'FLOORS_SHORT', 'HEATSRC_SHORT', 'HEATSYSTEM_SHORT',
									 'ROOF_SHORT', 'SEWER_SHORT', 'SIDING_SHORT', 'EXTERIOR_SHORT', 'BASEDESC_SHORT',
                                    'STYLE_SHORT', 'IMAGES'])

    @staticmethod
    def fill_na_values(df_var):

        df_var = df_var.astype('string')

        datatype_dict = {'ACRES': ['0.0', 'string'], 'AGENTLIST': ['000000', 'string'],
                         'ANTICCLOSEDDATE': ['00/00/0000 00:00:00', 'string'],
                         'BATHSTOTAL': ['0.0', 'float64'], 'BEDS': ['0', 'int64'],
                         'CLOSEDDATE': ['00/00/0000 00:00:00', 'string'],
                         'COUNTYCODE': ['00', 'string'], 'AGENTSELL': ['000000', 'string'],
                         'DAYSONMARKET': ['0.0', 'float64'], 'FIREPLACES': ['0', 'int64'],
                         'EXPIREDATE': ['00/00/0000 00:00:00', 'string'], 'GARAGECAP': ['0.0', 'float64'],
                         'LISTDATE': ['00/00/0000 00:00:00', 'string'], 'APPFEE': ['0.0', 'string'],
                         'LISTPRICE': ['0', 'int64'], 'LOANTERMS_SHORT': ['Unknown', 'string'],
                         'LOTSIZE': ['0x0', 'string'], 'MLSNUM': ['000000', 'string'],
                         'OFFICELIST': ['000000', 'string'], 'OFFICESELLNAME': ['NEW JERSEY', 'string'],
                         'ORIGLISTPRICE': ['0', 'int64'], 'OWNERNAME': ['Not Available', 'string'],
                         'PARKNBRAVAIL': ['0', 'int64'], 'EASEMENT_SHORT': ['U', 'string'],
                         'PENDINGDATE': ['00/00/0000 00:00:00', 'string'], 'ASSOCFEE': ['0.0', 'float64'],
                         'POOL_SHORT': ['N', 'string'], 'STYLEPRIMARY_SHORT': ['Unknown', 'string'], 'SUBPROPTYPE': ['U', 'string'],
                         'REMARKSAGENT': ['None', 'string'], 'REMARKSPUBLIC': ['None', 'string'], 'ROOMS': ['0.0', 'float64'],
                         'SALESPRICE': ['0', 'int64'], 'SHOWSPECIAL': ['None', 'string'],
                         'STREETNUMDISPLAY': ['0', 'string'], 'SUBDIVISION': ['None', 'string'],
                         'TAXID': ['0000-00000-0000-00000-0000', 'string'], 'TOWNCODE': ['0', 'string'],
                         'WITHDRAWNDATE': ['00/00/0000 00:00:00', 'string'],
                         'YEARBUILT': ['9999', 'int64'], 'ZIPCODE': ['00000', 'string'], 'SP/LP%': ['0%', 'string'],
                         'BASEMENT_SHORT': ['U', 'string'], 'BUSRELATION_SHORT': ['Unknown', 'string'],
                         'AGENTSELLNAME': ['NOT AVAILABLE', 'string'], 'OFFICESELL': ['000000', 'string'],
                         'LISTTYPE_SHORT': ['Unknown', 'string'], 'BASEDESC_SHORT': ['Unknown', 'string'],
                         'ASSESSAMOUNTBLDG': ['0.0', 'string'], 'ASSESSAMOUNTLAND': ['0.0', 'string'],
                         'ASSESSTOTAL': ['0.0', 'string'], 'COMPBUY': [None, 'string'], 'COMPSELL': [None, 'string'],
                         'COMPTRANS': [None, 'string'], 'ZONING': [None, 'string'], 'STYLE_SHORT': ['Unknown', 'string']}

        for col, default_data in datatype_dict.items():
            try:
                df_var[col].fillna(default_data[0], inplace=True)
                df_var[col] = df_var[col].astype(default_data[1])

            except ValueError:
                pass

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
    def fixer_upper(df_var):

        temp_df_ = df_var.copy()

        for idx, row in temp_df_.iterrows():

            if row['STYLEPRIMARY_SHORT'] == 'FixrUppr' or 'FixrUppr' in row['STYLE_SHORT'].split(','):

                df_var.loc[idx, 'CONDITION'] = 'Fixer Upper'
                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = True

            else:

                df_var.loc[idx, 'POTENTIAL_INVESTMENT'] = False


        return df_var

    @staticmethod
    def investment_label(df_var):

        df_var['INVESTMENT_SALE'] = (df_var['OWNERNAME']
                                         .str.contains('\,?\s?\,?l\s?l\s?c|Investment|Improvement|Builders|Inc\.?|Management|Corp\.?',
                                                       case=False, na=False, regex=True))

        return df_var

    @staticmethod
    def original_lp_diff(df_var):

        df_var['OLP/LP%'] = round(((df_var['LISTPRICE'] - df_var['ORIGLISTPRICE']) / df_var['ORIGLISTPRICE']) * 100, 0)
        df_var['SP/OLP%'] = round(((df_var['SALESPRICE'] - df_var['ORIGLISTPRICE']) / df_var['ORIGLISTPRICE']) * 100, 0)

        return df_var

    @staticmethod
    def parse_property_attr(df_var):

        attributes_dict = {
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
            'FLOORS_SHORT': {'WOOD_FLOORS': 'Wood',
                             'MARBLE_FLOORS': 'Marble',
                             'TILE_FLOORS': 'Tile',
                             'CARPET_FLOORS': 'Carpet',
                             'VINYL_FLOORS': 'Vinyl',
                             'LAMINATE_FLOORS': 'Laminate',
                             'STONE_FLOORS': 'Stone',
                             'PARQUET_FLOORS': 'Parquet'
                             },
            'HEATSRC_SHORT': {'HEAT_SRC_NATGAS': 'GasNatur',
                               'HEAT_SRC_ELECTRIC': 'Electric',
                               'HEAT_SRC_OILABV': 'OilAbIn',
                               'HEAT_SRC_OILBEL': 'OilBelow',
                               'HEAT_SRC_SOLAR': 'SolarLse'},
            'BASEDESC_SHORT': {'BASEDESC_BILCOSTY': 'BilcoSty',
                               'BASEDESC_FINISHED': 'Finished',
                               'BASEDESC_FINPART': 'FinPart',
                               'BASEDESC_FRNCHDRN': 'FrnchDrn',
                               'BASEDESC_FULL': 'Full',
                               'BASEDESC_PARTIAL': 'Partial',
                               'BASEDESC_SLAB': 'Slab',
                               'BASEDESC_UNFINISH': 'Unfinish',
                               'BASEDESC_WALKOUT': 'Walkout',
                               'BASEDESC_UNKNOWN': 'Unknown'},
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
            'ROOF_SHORT': {'ROOF_ASPHSHNG': 'AsphShng',
                           'ROOF_COMPSHNG': 'CompShng',
                           'ROOF_FLAT': 'Flat'},
            'SEWER_SHORT': {'SEWER_ASSOCTN': 'Assoctn',
                            'SEWER_PUBLAVAL': 'PublAval',
                            'SEWER_PUBLIC': 'Public',
                            'SEWER_SEPTIC': 'Septic'},
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
                             'SIDING_WOODSHNG': 'WoodShng'}
        }

        for target_col, values in attributes_dict.items():

            for new_col, pattern in values.items():

                df_var[new_col] = df_var[target_col].str.contains(pattern, case=True, na=False)

        return df_var

    @staticmethod
    def reorder_columns(df_var):

        location_dict = {
            'MLS': 1,
            'LATITUDE': 13,
            'LONGITUDE': 14,
            'CONDITION': 16,
            'OLP/LP%': 19,
            'SP/OLP%': 22,
            'INVESTMENT_SALE': 23,
            'POTENTIAL_INVESTMENT': 24,
            'SQFTAPPROX': 31,
            'ACRES': 32,
            'LOTSIZE': 33,
            'LOTSIZE (SQFT)': 34,
            'QTR': 43,
            'DAYS_TO_CLOSE': 47,
            'ANTIC_CLOSEDATE_DIFF': 48,
            'LISTING_REMARKS': df_var.shape[1] - 1,
        }

        for col, value in location_dict.items():
            df_var.insert(value, col, df_var.pop(col))

        return df_var

    @staticmethod
    def standard_cleaning(df_var):

        cleaning_dict = {
            '*': ['ACRES', 'BLOCKID', 'COUNTY', 'COUNTYCODE', 'LOTID', 'LOTSIZE',
                  'OWNERNAME', 'STREETNAME', 'TAXID', 'TOWNCODE', 'ZIPCODE'],
            '%': ['SP/LP%'],
            '\.?\*?\(\d{4}\*?\)': ['TOWN'],
            '00:00:00': ['ASSESSAMOUNTBLDG', 'APPFEE']
        }

        for char, column_list in cleaning_dict.items():

            for col in column_list:

                if char == '*' or char == '%':
                    df_var[col] = df_var[col].str.replace(char, '')
                elif char == '\.?\*?\(\d{4}\*?\)':
                    df_var[col] = df_var[col].str.replace(char, '', regex=True)
                elif char == '00:00:00':
                    df_var[col] = df_var[col].str.replace(char, '0.0')

        df_var['SP/LP%'] = df_var['SP/LP%'].astype('float64')
        df_var['SP/LP%'] = df_var['SP/LP%'] - 100.0

        return df_var

    @staticmethod
    def sub_property_type(df_var):

        temp_df_ = df_var.copy()
        target_styles = ['TwnIntUn', 'OneFloor', 'MultiFlr', 'TwnEndUn', 'FirstFlr', 'HighRise']

        for idx, row in temp_df_.iterrows():

            boolean_list = [True if i in target_styles else False for i in row['STYLE_SHORT'].split(',')]

            if row['STYLEPRIMARY_SHORT'] in  target_styles:

                if row['SUBPROPTYPE'] == 'CCT':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE'] = 'CCT'

            elif row['STYLEPRIMARY_SHORT'] not in target_styles and True in boolean_list:

                if row['SUBPROPTYPE'] == 'SinglFam':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE'] = 'SinglFam'

            else:
                if row['SUBPROPTYPE'] == 'SinglFam':
                    pass

                else:
                    df_var.loc[idx, 'SUBPROPTYPE'] = 'SinglFam'

        return df_var


if __name__ == '__main__':
    data_consumer = KafkaConsumer(
        client_id='Residential Consumer',
        group_id='Residential Consumer',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode('utf-8'),
        consumer_timeout_ms=120000
                                   )

    # Using the subscribe method instead of directly assigning a topic so Kafka can
    # handle the re-balancing of partitions for me
    data_consumer.subscribe(['res_properties'])

    final_df_list = []
    empty_data = 0
    data_available = True

    while data_available:

        try:
            new_data = data_consumer.poll(max_records=100, timeout_ms=5000)
            # print(data_consumer.assignment())
            if not new_data:
                empty_data += 1

                if empty_data == 10:
                    data_available = False
                    break
            elif new_data:

                for partition, messages in new_data.items():
                    df_list = []

                    for dataset in messages:

                        try:
                            json_obj = json.loads(json.loads(dataset.value))
                            df = pd.DataFrame(data=json_obj['data'], index=json_obj['index'], columns=json_obj['columns'])
                            # Add a filter that says if the 'BATHSFULLTOTAL' AND 'BATHSHALFTOTAL' cols are missing, continue
                            # if 'BATHSFULLTOTAL' or 'BATHSHALFTOTAL' not in df.columns:
                            #     continue

                            df_list.append(df)

                        except json.decoder.JSONDecodeError:
                            pass


                temp_df = pd.concat(df_list).reset_index(drop=True)
                final_df = (temp_df.pipe(KafkaGSMLSConsumer.fill_na_values)
                            .pipe(KafkaGSMLSConsumer.standard_cleaning)
                            .pipe(KafkaGSMLSConsumer.convert_lot_size)
                            .pipe(KafkaGSMLSConsumer.calculate_dates)
                            .pipe(KafkaGSMLSConsumer.combine_listing_remarks)
                            .pipe(KafkaGSMLSConsumer.parse_property_attr)
                            .pipe(KafkaGSMLSConsumer.investment_label)
                            .pipe(KafkaGSMLSConsumer.fixer_upper)
                            .pipe(KafkaGSMLSConsumer.sub_property_type)
                            .pipe(KafkaGSMLSConsumer.original_lp_diff)
                            .pipe(KafkaGSMLSConsumer.reorder_columns)
                            .pipe(KafkaGSMLSConsumer.drop_columns))
                final_df_list.append(final_df)

                if empty_data > 1:
                    empty_data = 0

        except ValueError as v:
            print(f'{v}')
            # data_consumer.commit()
            pass


    data_consumer.close()
    data_available = False
    df = pd.concat(final_df_list)
    df = df.drop_duplicates(subset=['STREETNUMDISPLAY', 'STREETNAME', 'TOWN', 'LISTDATE'], keep='last')

    current_wd = os.getcwd()
    os.chdir('F:\\Real Estate Investing')
    df.to_excel('Kafka_Res_Properties_Backup.xlsx', index=False)
    os.chdir(current_wd)
