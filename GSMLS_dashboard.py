import pandas as pd
import numpy as np
import os
import logging
import streamlit as st
from streamlit_folium import st_folium, folium_static
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
import json
import geopandas as gpd
import folium
import re
import requests
import time
from requests.exceptions import ConnectionError
from sqlalchemy.exc import OperationalError
from collections import defaultdict
import joblib
import tensorflow as tf
from category_encoders import CountEncoder
import scipy.stats as stats
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
from sklearn.preprocessing import PowerTransformer

# ------------------------------------------------------------------------------------------
#                           USE THIS SECTION FOR LOGGER FUNCTIONS
# ------------------------------------------------------------------------------------------


class KafkaLoggingHandler(logging.Handler):
    pass


def dashboard_logger():

    logger = logging.getLogger('GSMLS_Dashboard')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if not logger.handlers:

        # Create the Kafka Handler logger
        kh_handler = KafkaLoggingHandler()
        kh_handler.setLevel(logging.INFO)

        # Create formatting for the logger
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%d-%b-%y %H:%M:%S')
        kh_handler.setFormatter(formatter)
        logger.addHandler(kh_handler)

    return logger


# ------------------------------------------------------------------------------------------
#                           USE THIS SECTION FOR REGULAR FUNCTIONS
# ------------------------------------------------------------------------------------------


def add_comp_properties(marker_group, property_details: pd.Series = None):

    if property_details is not None:

        comp_properties = st.session_state['comp_data']
        mun_code = comp_properties['NJ_TOWNCODE'].unique().tolist()[0]

        for _, data in comp_properties.iterrows():

            try:
                create_similar_comp(property_details, data).add_to(marker_group)

            except TypeError:
                pass

            # except JSONDecodeError:
            #     pass

        return marker_group, mun_code


def assertion_check():

    check_dict = {
        1: ['rooms', 'bedrooms', 'bathrooms'],
        '00000': ['zipcode'],
        50000: ['listprice'],
        None: ['address', 'municipality', 'county', 'primary_style']
    }

    for key, value_list in check_dict.items():

        for item_ in value_list:

            if key == 1 or key == 50000:
                assert st.session_state[item_] >= key, f'{item_.title()} is not greater than {key}. Please try again.'

            elif key == '00000':
                assert st.session_state[item_] != key, f'Invalid {item_.title()}. Please try again.'

            elif key is None:
                assert st.session_state[item_] is not key, f'{item_.title()} has not been completed. Please try again.'


def calculate_sales_rate(obj):

    total_comp_props = len(obj)
    comp_data = obj[obj['STATUS_SHORT'] == 'SD']
    sold_comp_prop = len(comp_data)

    return comp_data, sold_comp_prop / total_comp_props


def check_property_values(targ_dict: dict):

    if (targ_dict['year_built'][0] == 1680 or targ_dict['lot_size'][0] == 0
            or targ_dict['sq_ft'][0] == 0 or targ_dict['total_assess'][0] == 0
            or targ_dict['taxes'][0] == 0):

        default_values = {
            'year_built': 1680,
            'total_assess': 0,
            'lot_size': 0,
            'sq_ft': 0,
            'taxes': 0
        }

        partial_address = targ_dict['address'][0][:8]

        municipal_info = get_municipal_data(targ_dict['town'][0], targ_dict['county'][0])
        # st.session_state['municipal_info'] = municipal_info
        municipal_code = municipal_info.loc['MUN_CODE'].values[0]
        current_year = datetime.today().year

        try:

            tax_data = query_tax_data(partial_address, municipal_code)

        except AssertionError:

            tax_data = municipal_info.copy()

        for key, value in default_values.items():

            if targ_dict[key][0] == value:

                if key == 'year_built':
                    targ_dict[key][0] = tax_data.loc[key].values[0]

                elif key == 'lot_size':
                    try:
                        targ_dict[key][0] = tax_data.loc['acreage'].values[0] * 43560
                    except KeyError:
                        targ_dict[key][0] = tax_data.loc['LOTSIZE (SQFT)'].values[0]
                elif key == 'total_assess':
                    targ_dict[key][0] = municipal_info.loc['ASSESSTOTAL'].values[0]

                elif key == 'sq_ft':
                    val = tax_data.loc['building_sqft'].values[0]

                    if val == 0 or val is None or str(val) == 'nan':
                        default_sqft = municipal_info.loc['building_sqft'].values[0]

                        if default_sqft is None or str(default_sqft) == 'nan' or default_sqft == 0:
                            targ_dict[key][0] = st.session_state['mean_sqft']
                        else:
                            targ_dict[key][0] = municipal_info.loc['building_sqft'].values[0]
                    else:
                        targ_dict[key][0] = tax_data.loc['building_sqft'].values[0]

                elif key == 'taxes':
                    targ_dict[key][0] = municipal_info.loc['TAXAMOUNT'].values[0]

        targ_dict['assess_lp_ratio'][0] = targ_dict['total_assess'][0] / targ_dict['listprice'][0]
        targ_dict['property_valuation'][0] = property_valuation(targ_dict['assess_lp_ratio'][0])
        targ_dict['age_of_property'][0] = int(current_year) - int(targ_dict['year_built'][0])

        # Ensure that the property age isn't greater than the maximum possible
        # Maximum property age = current year - 1680
        assert targ_dict['age_of_property'][0] <= int(current_year) - 1680, 'Property age is greater than the maximum limit. Please input values again'
        assert targ_dict['age_of_property'][0] >= 0, 'Property age is less than zero. Please input values again'

        return targ_dict

    else:

        return targ_dict


def construct_full_pipeline():

    power_maxabs_pipeline = Pipeline([
        ("select_cols", FunctionTransformer(power_maxabs_column_selector)),
        ("power", PowerTransformer(method='yeo-johnson')),
        ("scale", MaxAbsScaler())
    ])

    maxabs_only_pipeline = Pipeline([
        ("select_cols", FunctionTransformer(maxabs_only_column_selector)),
        ("scale", MaxAbsScaler())
    ])

    count_encoder_pipeline = Pipeline([
        ("select_cols", FunctionTransformer(count_encoder_column_selector)),
        ("encoder", CountEncoder(normalize=True)),
        ("scale", MaxAbsScaler())
    ])

    full_pipeline = FeatureUnion([
        ("power_minmax", power_maxabs_pipeline),
        ("minmax_only", maxabs_only_pipeline),
        ("count_encoder", count_encoder_pipeline),
    ]).set_output(transform='pandas')

    return full_pipeline


def count_encoder_column_selector(X):

    return X[['STYLEPRIMARY_SHORT']]


def create_address(address_, town_, zipcode_):

    return f'{address_}, {town_}, NJ {zipcode_}'


@st.cache_data(ttl=timedelta(days=7))
def create_agg_data():

    data = st.session_state['market_data']

    agg_df = data.groupby(['COUNTY', 'TOWN', 'NJ_TOWNCODE', 'MONTH', 'YEAR', 'STATUS_SHORT']).agg(
        {
            'DAYSONMARKET': 'mean',
            'LISTPRICE': 'mean',
            'SALESPRICE': 'mean',
            'SP/LP%': 'mean',
            'TAXAMOUNT': 'mean',
            'LOTSIZE (SQFT)': 'mean',
            'building_sqft': 'mean',
            'year_built': 'mean',
            'ASSESSTOTAL': 'mean',
            'MLSNUM': 'count'
        }
        )

    agg_df.reset_index(inplace=True)
    agg_df.sort_values(by=['YEAR', 'MONTH', 'TOWN'], ascending=True, inplace=True)
    agg_df.rename(columns={'TOWN': 'MUN', 'NJ_TOWNCODE': 'MUN_CODE'}, inplace=True)

    return agg_df, round(agg_df['building_sqft'].mean(), 0)


def create_base_map(property_details: pd.Series = None, default_location=(40.05832, -74.40566), default_zoom_start=7.5):

    radius_dict = {
        '0.25 miles': {'fill_color': 'green', 'fill_opacity': 0.5, 'radius': 402.336},
        '0.5 miles': {'fill_color': 'green', 'fill_opacity': 0.25, 'radius': 804.672},
        '1 mile': {'fill_color': 'blue', 'fill_opacity': 0.25, 'radius': 1609.344},
        '2 miles': {'fill_color': 'blue', 'fill_opacity': 0.20, 'radius': 3218.688}
    }

    property_group = folium.FeatureGroup(name='Properties', control=False, overlay=True)

    # Create a base map
    if property_details is None:
        base_map = folium.Map(location=default_location, zoom_start=default_zoom_start)
    else:
        lat, lon = property_details['LATITUDE'], property_details['LONGITUDE']
        base_map = folium.Map(location=[lat, lon], zoom_start=13)
        folium.LayerControl().add_to(base_map)

        target_property_location = [lat, lon]
        popup = create_popup(target_prop=property_details)

        folium.CircleMarker(
            location=target_property_location,
            tooltip=create_address(property_details['ADDRESS'], property_details['TOWN'], property_details['ZIPCODE']),
            popup=popup,
            radius=5,  # Pixel radius
            # color='black', # Color of the outline of the shape
            stroke=False,  # Controls if shape has an outline
            fill=True,
            fill_color='black',
            fill_opacity=0.7,
            zIndex=9999
          ).add_to(property_group)

        # Add a circle radius for nearest comps
        for key, args in radius_dict.items():
            folium.Circle(
                location=target_property_location,
                radius=args['radius'],
                tooltip=key,
                # color='black',
                fill_color=args['fill_color'],
                fill=True,
                stroke=False,
                fill_opacity=args['fill_opacity']
            ).add_to(base_map)

    return base_map, property_group


def create_choropleth(nj_map):

    # Merge the geojson data and market data to make creation of the Choropleth map easier
    nj_geo_df = st.session_state['choropleth_data']

    # """Create a seperate tooltip object and tooltip layer to place on the choropleth map
    # I currently have null values in the data which interrupts the natural function of the
    # choropleth function tooltip arg
    # """

    tooltip = folium.features.GeoJsonTooltip(
        fields=['MUN', 'COUNTY', 'SALESPRICE', 'NEW LISTINGS', 'CLOSED SALES',
                'DAYSONMARKET', 'SP/LP%', 'INVENTORY', 'MONTHS OF SUPPLY'],
        aliases=['Municipality:', 'County:', 'Median Sales Price:', 'New Listings:',
                 'Closed Sales:', 'Days on Market:', 'SP/LP%:',
                 'Available Inventory:', 'Months of Supply:'],
        localize=True,
        sticky=False,
        labels=True,
        style="""
            background-color: #F0EFEF;
            border: 2px solid black;
            border-radius: 3px;
            box-shadow: 3px;
        """,
    )

    tooltip_layer = folium.GeoJson(
        nj_geo_df,
        style_function=lambda feature: {
            'fillColor': 'transparent',
            'color': 'black',
            'weight': 0.5
        },
        tooltip=tooltip
    )

    folium.Choropleth(
        geo_data=nj_geo_df,
        data=nj_geo_df[['MUN', 'SALESPRICE']],
        columns=['MUN', 'SALESPRICE'],
        key_on='feature.properties.MUN',
        fill_color='YlGnBu',
        fill_opacity=1,
        line_opacity=0.7,
        legend_name='Mean Sales Prices in NJ',
        highlight=True,
        line_color='black',
        line_weight=1,
        nan_fill_color='purple',
        nan_fill_opacity=0.4,
        bins=nj_geo_df['SALESPRICE'].quantile((0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1)).tolist(),
        show=True
    ).add_to(nj_map)

    tooltip_layer.add_to(nj_map)

    return nj_map


@st.cache_data(ttl=timedelta(days=7))
def create_choropleth_data():

    nj_geo_df = st.session_state['geojson']
    ncjar_data = st.session_state['ncjar_data']
    ncjar_data = ncjar_data[ncjar_data['MONTH'] == ncjar_data['MONTH'].max()]
    ncjar_data['MUN'] = ncjar_data['MUN'].str.upper()
    ncjar_data['COUNTY'] = ncjar_data['COUNTY'].str.upper()

    # Merge the geojson data and ncjar data to make creation of the Choropleth map easier
    return nj_geo_df.merge(ncjar_data, on=['MUN', 'COUNTY'], how='left')


def create_comp_map(property_details: pd.Series = None):

    # geo_data = st.session_state['geojson']
    final_map, property_group = create_base_map(property_details)
    final_group, mun_code = add_comp_properties(marker_group=property_group, property_details=property_details)
    final_group.add_to(final_map)

    return final_map


def create_data_query():

    if st.session_state['start_year'] != st.session_state['end_year']:
        # Send nested query
        pass
    else:
        query = (f'''SELECT "MLSNUM", "STATUS_SHORT", "ADDRESS", "TOWN", "COUNTY", "ZIPCODE", "NJ_TOWNCODE", "MONTH", 
            "LISTPRICE", "OLP/LP%%", "SALESPRICE", "SP/LP%%", "LISTDATE", "CLOSEDDATE", year_built,
            "STYLEPRIMARY_SHORT", "ROOMS", "BEDS", "BATHSTOTAL", building_sqft, "LOTSIZE (SQFT)",
            "DAYSONMARKET", "CONDITION", "DISTRESSED_SALE", "TAXAMOUNT", "LATITUDE", 
            "LONGITUDE", "ASSESSTOTAL", "YEAR" FROM gsmls_imputed_data WHERE "YEAR" = \'{st.session_state['end_year']}\' 
            AND "MONTH" BETWEEN {st.session_state['start_month']} AND {st.session_state['end_month']};''')

        return query


def create_municipality_list():

    data = st.session_state['state_data']

    town_list = data[data['County'] == st.session_state['county']]['Municipality'].unique().tolist()

    st.session_state['municipality_list'] = town_list


def create_popup(target_prop: pd.Series = None, comp_details: pd.Series = None):

    if target_prop is None:
        property_details = comp_details.loc[["STATUS_SHORT", "CONDITION", "STYLEPRIMARY_SHORT", "ADDRESS", "LISTPRICE",
                                            "OLP/LP%", "SALESPRICE", "SP/LP%", "ROOMS", "BEDS", "BATHSTOTAL", "building_sqft",
                                            "CLOSEDDATE", "DAYSONMARKET"]]
    else:
        property_details = target_prop.loc[
            ["STATUS_SHORT", "CONDITION", "STYLEPRIMARY_SHORT", "ADDRESS", "LISTPRICE",
             "OLP/LP%", "ROOMS", "BEDS", "BATHROOMS", "SQ_FT", "DAYSONMARKET", "LISTDATE"]]

    html = property_details.to_frame().to_html()

    popup = folium.Popup(folium.Html(html, script=True), max_width=500)

    return popup


def create_similar_comp(target_property: pd.Series, comp_property: pd.Series):

    # Create Boolean variables to guage target property and comp similarities
    room_diff = abs(target_property['ROOMS'] - comp_property['ROOMS'])
    sqft_diff = abs(target_property['SQ_FT'] - comp_property['building_sqft'])
    similar_style = target_property['STYLEPRIMARY_SHORT'] == comp_property['STYLEPRIMARY_SHORT']
    similar_baths = float(target_property['BATHROOMS']) == comp_property['BATHSTOTAL']
    similar_rooms = room_diff < 3

    if 0 <= target_property['SQ_FT'] <= 1000:
        similar_sqft = sqft_diff < (target_property['SQ_FT'] * 0.25)

    elif 1001 <= target_property['SQ_FT'] <= 2000:
        similar_sqft = sqft_diff < (target_property['SQ_FT'] * 0.20)

    elif 2001 <= target_property['SQ_FT'] <= 3500:
        similar_sqft = sqft_diff < (target_property['SQ_FT'] * 0.15)

    elif 3501 <= target_property['SQ_FT'] <= 5000:
        similar_sqft = sqft_diff < (target_property['SQ_FT'] * 0.10)

    elif target_property['SQ_FT'] >= 5001:
        similar_sqft = sqft_diff < (target_property['SQ_FT'] * 0.10)

    popup = create_popup(comp_details=comp_property)
    # st.write(f"{comp_property['ADDRESS']}: {similar_style}, {similar_sqft}, {similar_rooms}, {similar_baths}")

    if similar_style is True:

        if similar_baths and similar_rooms and similar_sqft:

            circle_marker = folium.CircleMarker(
                    location=[comp_property['PROP_LATITUDE'], comp_property['PROP_LONGITUDE']],
                    tooltip=create_address(comp_property['ADDRESS'], comp_property['TOWN'], comp_property['ZIPCODE']),
                    popup=popup,
                    radius=5,
                    # color='black',
                    stroke=False,
                    fill=True,
                    fill_color='blue',
                    fill_opacity=0.7
                )

            return circle_marker

        else:

            circle_marker = folium.CircleMarker(
                    location=[comp_property['PROP_LATITUDE'], comp_property['PROP_LONGITUDE']],
                    tooltip=create_address(comp_property['ADDRESS'], comp_property['TOWN'], comp_property['ZIPCODE']),
                    popup=popup,
                    radius=5,
                    # color='black',
                    stroke=False,
                    fill=True,
                    fill_color='yellow',
                    fill_opacity=0.7
                )

            return circle_marker

    else:

        circle_marker = folium.CircleMarker(
                    location=[comp_property['PROP_LATITUDE'], comp_property['PROP_LONGITUDE']],
                    tooltip=create_address(comp_property['ADDRESS'], comp_property['TOWN'], comp_property['ZIPCODE']),
                    popup=popup,
                    radius=5,
                    # color='black',
                    stroke=False,
                    fill=True,
                    fill_color='red',
                    fill_opacity=0.7
                )

        return circle_marker


def create_target_property(prev_run):

    if prev_run is None:

        ld = st.session_state['listdate']
        month_ = int(ld.month)
        year_ = int(ld.year)
        ld_datetime = datetime.combine(ld, datetime.min.time())
        dom = datetime.today() - ld_datetime

        assertion_check()

        targ_prop = {
            'status_short': ['A'],
            'address': [st.session_state['address']],
            'town': [st.session_state['municipality']],
            'county': [st.session_state['county']],
            'zipcode': [st.session_state['zipcode']],
            'listprice': [st.session_state['listprice']],
            'olp/lp%': [st.session_state['olp/lp%']],
            'year_built': [st.session_state['year_built']],
            'styleprimary_short': [st.session_state['primary_style']],
            'rooms': [st.session_state['rooms']],
            'beds': [st.session_state['bedrooms']],
            'bathrooms': [st.session_state['bathrooms']],
            'fireplaces': [st.session_state['fireplaces']],
            'garagecap': [st.session_state['garagecap']],
            'sq_ft': [st.session_state['sq_ft']],
            'daysonmarket': [dom.days],
            'condition': [st.session_state['condition']],
            'taxes': [st.session_state['taxes']],
            'listdate': [st.session_state['listdate']],
            'month': [month_],
            'year': [year_],
            'lot_size': [st.session_state['lot_size']],
            'total_assess': [st.session_state['assesstotal']],
            'assess_lp_ratio': [st.session_state['assesstotal'] / st.session_state['listprice']],
            'property_valuation': [0.0],
            'age_of_property': [0],
            'mortgage_rate': [st.session_state['cur_mortgage_rate']],
            'sales_rate': [0],
            'latitude': [0.0],
            'longitude': [0.0],
            'cls_prediction': [0.0],
            'reg_prediction': [0],
            'pro_remarks': [''],
            'con_remarks': ['']
        }

        targ_prop = check_property_values(targ_prop)

        # Ensure the prev_address state is None or successful message won't be initiated
        if st.session_state['prev_address'] is not None:
            st.session_state['prev_address'] = None

    else:

        # Assures that the previous address isn't None if this block is accessed
        # Adds a Null value to the previous run property list which isn't useful
        assert st.session_state['prev_address'] is not None, 'Previous Address was not captured. Please try again...'

        targ_prop = st.session_state['previously_run'][st.session_state['prev_address']]
        st.session_state['municipality'] = targ_prop['town'][0]

    test_data = st.session_state['market_data']
    list_date = pd.to_datetime(targ_prop['listdate'][0])
    county_ = targ_prop['county'][0].replace(' County', '')

    comp_data = test_data.loc[(test_data['TOWN'] == st.session_state['municipality'])
                                                  & (test_data['COUNTY'] == county_)
                                                  & (test_data['CLOSEDDATE'] >= (list_date - pd.Timedelta(days=90)))]

    # Calculate the sales rate of the municipality
    comp_data, sr = calculate_sales_rate(comp_data)
    targ_prop['sales_rate'][0] = sr
    st.session_state['comp_data'] = comp_data.sort_values(by='LISTDATE', ascending=False)

    if st.session_state['prev_address'] is None:
        targ_prop['latitude'][0], targ_prop['longitude'][0] = query_geocode(targ_prop['address'][0],
                                                        targ_prop['town'][0], targ_prop['zipcode'][0])
        prep_data = prep_target_data(targ_prop)
        targ_prop = make_cls_prediction(prep_data, targ_prop)
        targ_prop = make_reg_prediction(prep_data, targ_prop)
        st.session_state['target_property'] = targ_prop
        st.session_state['previously_run'][st.session_state['address']] = targ_prop

    df = pd.DataFrame(targ_prop)
    df.columns = df.columns.str.upper()

    return df.squeeze()


@st.cache_data
def default_map():

    """
    Create a choropleth map using the NJ GeoJson, Folium and the aggregate data

    :return:
    """

    # Create the necessary data and map objects
    nj_map = folium.Map(location=[40.05832, -74.40566], zoom_start=7.5)

    # Add the CartoDB positron tilelayer, choropleth layer then tooltip layer
    folium.TileLayer('CartoDB positron', name="Light Map", control=False).add_to(nj_map)

    return nj_map


def geocode_api():

    return '68127e4c57402180688960nth077850'


def get_comps(prev_run):

    try:
        targ_prop = create_target_property(prev_run)

        final_map = create_comp_map(targ_prop)
        st.session_state['get_comps'] = True
        st.session_state['reset'] = False
        st.session_state['comp_map'] = final_map

    except AssertionError as e:
        # Resets the dashboard if the AssertionError is raised
        st.warning(f'{e}', icon="⚠️")
        reset()

    except (ValueError, TypeError, ZeroDivisionError):
        bad_item = st.session_state['previously_run'].popitem()
        bad_address = bad_item[0]
        st.error(f'{bad_address} could not be found. Please re-enter the address', icon="🚨")

    except (ConnectionError, OperationalError):
        st.error(f'ConnectionError occurred. Please re-enter the address', icon="🚨")

    else:
        if st.session_state['prev_address'] is None:
            st.toast(f"Comparables for {st.session_state['address']} have been run successfuly!", icon='✅')
            st.toast(f"Comparable history can be accessed from the Previously Run Comparables list")


def get_current_and_prev_month():

    end_date = datetime.now()
    start_date = end_date - timedelta(days=120)

    if start_date.year == end_date.year:
        st.session_state['start_year'] = None
        st.session_state['end_year'] = end_date.year
        st.session_state['start_month'] = start_date.month
        st.session_state['end_month'] = end_date.month

    else:
        st.session_state['start_year'] = start_date.year
        st.session_state['end_year'] = end_date.year
        st.session_state['start_month'] = start_date.month
        st.session_state['end_month'] = end_date.month


def get_mortgage_rates():
    """Use this function to get the mortgage rates from fred"""

    current_date = datetime.today()
    year_ = current_date.year
    month_ = str(current_date.month)

    if len(month_) == 1:
        month_ = '0' + month_

    freds_api_key = 'cc3566d9932429ade33077a7ecddc5a7'
    freds_api = (f'https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&api_key={freds_api_key}'
                 f'&file_type=json&observation_start={year_}-01-01&observation_end=9999-12-31&sort_order=desc'
                 f'&limit=2')

    mortgage_data = requests.get(freds_api)

    if mortgage_data.status_code == 200:
        mortgage_json = json.loads(mortgage_data.text)
        mortgage_rate1 = pd.Series(mortgage_json['observations'][0].values(),
                                   index=mortgage_json['observations'][0].keys())
        mortgage_rate2 = pd.Series(mortgage_json['observations'][1].values(),
                                   index=mortgage_json['observations'][1].keys())

        return float(mortgage_rate1['value']), float(mortgage_rate2['value'])

    else:

        raise AttributeError('Mortgage Data Not Queried')


def get_municipal_data(town_name, county_):

    county_ = county_.replace(' County', '')
    df = st.session_state['aggregate_data']
    mask = df[(df['MUN'] == town_name) & (df['COUNTY'] == county_)]
    municipal_info = df.loc[mask.index]

    return municipal_info.T


@st.cache_data
def load_cls_model():

    model_path = 'C:\\Users\\jibreel.q.hameed\\PycharmProjects\\pythonProject\\Real Estate Analysis'
    full_model_dir = os.path.join(model_path, 'GSMLS_SimpleCls_DNN.keras')

    return tf.keras.models.load_model(full_model_dir)


@st.cache_data
def load_nj_geojson():

    return gpd.read_file('NJ_Municipal_Boundaries.geojson')


@st.cache_data
def load_reg_model():
    model_path = 'C:\\Users\\jibreel.q.hameed\\PycharmProjects\\pythonProject\\Real Estate Analysis'
    full_model_dir = os.path.join(model_path, 'GSMLS_SimpleReg_DNN.keras')

    return tf.keras.models.load_model(full_model_dir,
                                      custom_objects={"masked_mse_metric": masked_mse_metric})


@st.cache_data
def load_targ_price_pipeline():

    return joblib.load('target_price_pipeline.joblib')


@st.cache_data
def load_transform_pipeline():

    return joblib.load('prefit_pipeline.joblib')


def likelihood_factors():

    target_prop = st.session_state['target_property']
    comp_props = st.session_state['comp_data']

    comp_props.rename(columns={'BATHSTOTAL': 'BATHROOMS', 'building_sqft': 'SQ_FT',
                               'year_built': 'YEAR_BUILT'}, inplace=True)
    pt = PowerTransformer(method='yeo-johnson', standardize=True)
    standard_scaler = StandardScaler()
    sample_count = comp_props.shape[0]

    possible_factors = {'daysonmarket': ('{}% of properties in {} have spent {} {} days on the market. This could '
                                         'signal {}.'),
                        'listprice': ('{}% of properties in {} have a list price {} {}. This could indicate '
                                      'the property is {}.'),
                        'beds': '{}% of properties in {} have a bedroom count {} {} bedrooms.',
                        'bathrooms': '{}% of properties in {} have a bathroom count {} {} bathrooms.',
                        'sq_ft': ('{}% of properties in {} have a livable square footage {} {} square feet. '
                                  'This may mean {} properties in this municipality.'),
                        'year_built': ('{}% of properties in {} were built {} than the year {}. '
                                       'This could possibly mean {}.')}
    pros = []
    cons = []

    for factor in possible_factors:

        data = comp_props[factor.upper()]

        if data.skew() > 0.5 or data.skew() < -0.5:
            pt.fit_transform(data.values.reshape(-1, 1))
            target_normal = float(pt.transform([[target_prop[factor][0]]])[0][0])
        else:
            standard_scaler.fit_transform(data.values.reshape(-1, 1))
            target_normal = float(standard_scaler.transform([[target_prop[factor][0]]])[0][0])

        p_value_t = round(stats.t.cdf(target_normal, sample_count - 1) * 100, 2)

        if target_normal <= -1:
            if factor == 'daysonmarket':
                pros.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], "greater than",
                    float(target_prop[factor][0]), "that the property is newly listed"))
            elif factor == 'listprice':
                pros.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], 'greater than',
                    f"${price_formatter(str(target_prop[factor][0]))}", "purposely underpriced to generate interest. "
                                            "This could also indicate some underlying issues with the property"))
            elif factor == 'beds':
                cons.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], 'greater than', float(target_prop[factor][0])))
            elif factor == 'bathrooms':
                cons.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], 'greater than', target_prop[factor][0]))
            elif factor == 'sq_ft':
                cons.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], 'greater than',
                    target_prop[factor][0], "this property provides less space than most"))
            elif factor == 'year_built':
                cons.append(possible_factors[factor].format(
                    100 - p_value_t, target_prop['town'][0], 'later',
                    target_prop[factor][0], "the target property has more wear and tear and requires updating"))
        elif target_normal >= 1:
            if factor == 'daysonmarket':
                cons.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], "less than",
                    float(target_prop[factor][0]), "this property has had less interest than most of the ones sold"))
            elif factor == 'listprice':
                cons.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], 'less than',
                    f"${price_formatter(str(target_prop[factor][0]))}", "overpriced"))
            elif factor == 'beds':
                pros.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], 'less than', float(target_prop[factor][0])))
            elif factor == 'bathrooms':
                pros.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], 'less than', target_prop[factor][0]))
            elif factor == 'sq_ft':
                pros.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], 'less than',
                    target_prop[factor][0], "this property provides more space than most"))
            elif factor == 'year_built':
                pros.append(possible_factors[factor].format(
                    p_value_t, target_prop['town'][0], 'earlier',
                    target_prop[factor][0], "the target property is fairly new and might be sold at a premium"))
        else:
            pass

    pro_remarks = '\n - '.join([i if pros != [] else None for i in pros])
    con_remarks = '\n - '.join([i if cons != [] else None for i in cons])

    st.session_state['previously_run'][st.session_state['address']]['pro_remarks'][0] = pro_remarks
    st.session_state['previously_run'][st.session_state['address']]['con_remarks'][0] = con_remarks


def make_cls_prediction(prep_data, raw_data):

    cols = ['DAYSONMARKET', 'BATHSTOTAL', 'YEARBUILT',
            'assess_to_lp_ratio', 'OLP/LP%', 'AGE_OF_PROPERTY',
            'LISTPRICE', 'ROOMS', 'property_valuation',
            'LOTSIZE (SQFT)', 'SQFTAPPROX', 'FIREPLACES',
            'TAXAMOUNT', 'ASSESSTOTAL', 'MONTH', 'LATITUDE', 'YEAR', 'GARAGECAP',
            'LONGITUDE', 'sales_rate', 'BEDS', 'MORTGAGE_RATE', 'STYLEPRIMARY_SHORT']

    loaded_pipeline = st.session_state['transform_pipeline']
    model = st.session_state['cls_model']

    # Filter out the columns not used
    target_data = prep_data[[i for i in prep_data if i in cols]]

    # # Transform the data to be used in the model
    transformed_data = loaded_pipeline.transform(target_data)

    # Make the predictions
    predictions = model.predict(transformed_data)

    raw_data['cls_prediction'][0] = predictions[0][0]

    return raw_data


def make_reg_prediction(prep_data, raw_data):

    cols = ['DAYSONMARKET', 'BATHSTOTAL', 'YEARBUILT',
            'assess_to_lp_ratio', 'OLP/LP%', 'AGE_OF_PROPERTY',
            'LISTPRICE', 'ROOMS', 'property_valuation',
            'LOTSIZE (SQFT)', 'SQFTAPPROX', 'FIREPLACES',
            'TAXAMOUNT', 'ASSESSTOTAL', 'MONTH', 'LATITUDE', 'YEAR', 'GARAGECAP',
            'LONGITUDE', 'sales_rate', 'BEDS', 'MORTGAGE_RATE', 'STYLEPRIMARY_SHORT',
            'property_valuation']

    loaded_pipeline = st.session_state['transform_pipeline']
    targ_price_pipeline = st.session_state['targ_price_pipeline']
    model = st.session_state['reg_model']

    # Filter out the columns not used
    target_data = prep_data[[i for i in prep_data if i in cols]]

    # # Transform the data to be used in the model
    transformed_data = loaded_pipeline.transform(target_data)
    full_data = np.column_stack((transformed_data, round(raw_data['cls_prediction'][0], 0)))

    # Make the predictions
    predictions = model.predict(full_data)
    predicted_price = targ_price_pipeline.inverse_transform(predictions.reshape(-1, 1))
    raw_data['reg_prediction'][0] = round(predicted_price[0][0], 2)

    return raw_data


# Create custom loss function to help evaluate the DNN better
def masked_mse_metric(y_true, y_pred, sample_weight=None):

    actual_prices = y_true[:, 0]
    sold_flags = y_true[:, 1]

    # Calculate the squared error
    square_error = tf.square(actual_prices - tf.squeeze(y_pred))

    # Apply the mask to ignore losses when properties arent sold
    masked_squared_error = square_error * sold_flags

    # Compute the mean only over the active sold samples (avoid dividing by the full batch size)
    # Add a small epsilon to avoid dividing by zero
    total_weight = tf.reduce_sum(sold_flags * (sample_weight if sample_weight is not None else 1.0)) + 1e-6
    mean_square_error = tf.reduce_sum(masked_squared_error) / total_weight

    # return masked_squared_error
    return mean_square_error


def maxabs_only_column_selector(X):

    max_abs_only_cols = ['MONTH', 'LATITUDE', 'YEAR', 'GARAGECAP',
                         'LONGITUDE', 'sales_rate', 'BEDS', 'MORTGAGE_RATE']

    return X[max_abs_only_cols]


@st.cache_resource(ttl=300.0)
def njtax_connection():

    # Connection for sqlalchemy to get data
    return create_engine(f"postgresql+psycopg2://postgres:Xy14RNw02SmD@database-1."
                         f"chuq28s6itob.us-east-2.rds.amazonaws.com:5432/nj_tax_assessor",
                         pool_recycle=300)


@st.cache_resource(ttl=300.0)
def njrealtor_connection():

    # Connection for sqlalchemy to get data
    return create_engine(f"postgresql+psycopg2://postgres:Xy14RNw02SmD@database-1."
                         f"chuq28s6itob.us-east-2.rds.amazonaws.com:5432/nj_realtor_data",
                         pool_recycle=300)


def power_maxabs_column_selector(X):

    power_minmax_cols = ['DAYSONMARKET', 'BATHSTOTAL', 'YEARBUILT',
                         'assess_to_lp_ratio', 'OLP/LP%', 'AGE_OF_PROPERTY',
                         'LISTPRICE', 'ROOMS', 'property_valuation',
                         'LOTSIZE (SQFT)', 'SQFTAPPROX', 'FIREPLACES',
                         'TAXAMOUNT', 'ASSESSTOTAL']

    return X[power_minmax_cols]


def prep_target_data(raw_data):

    df = pd.DataFrame(data=raw_data)
    df.columns = df.columns.str.upper()
    df = df.rename(columns={'YEAR_BUILT': 'YEARBUILT',
                            'ASSESS_LP_RATIO': 'assess_to_lp_ratio',
                            'BATHROOMS': 'BATHSTOTAL',
                            'PROPERTY_VALUATION': 'property_valuation',
                            'LOT_SIZE': 'LOTSIZE (SQFT)',
                            'SQ_FT': 'SQFTAPPROX',
                            'TAXES': 'TAXAMOUNT',
                            'TOTAL_ASSESS': 'ASSESSTOTAL',
                            'SALES_RATE': 'sales_rate'
        })

    # df = df[[i for i in df.columns if i not in ['STATUS_SHORT', 'ADDRESS', 'TOWN', 'COUNTY', 'ZIPCODE',
    #                                             'CONDITION', 'LISTDATE']]]

    return df


def price_formatter(value: str):

    if '.' in value:
        price_list = value.split('.')
        dollars = list(price_list[0])
        cents = price_list[1]
    else:
        dollars = list(value)
        cents = '00'

    formatted_dollar_list = []
    dollars.reverse()

    for idx, num in enumerate(dollars):
        if -idx in [-3, -6, -9, -12]:
            formatted_dollar_list.insert(0, ',')
            formatted_dollar_list.insert(0, num)

        else:
            formatted_dollar_list.insert(0, num)

    return ''.join(formatted_dollar_list) + '.' + cents


def primary_style_list():

    data = st.session_state['market_data']

    style_list = data['STYLEPRIMARY_SHORT'].unique().tolist()

    return style_list


def property_valuation(val):

    if val < -1.842767:  # The lower the assess_to_lp_zscore, the more overvalued the property

        return -1

    elif val > 1.724696:  # The higher the assess_to_lp_zscore, the more undervalued the property

        return 1

    else:

        return 0


def query_geocode(address_, town_, zipcode_):

    pass_list = ['Jersey City']
    remove_pattern = re.compile(r'Town|Twp|Boro|Village|City')

    if town_ not in pass_list:
        town_ = re.sub(remove_pattern, '', town_)

    address_ = address_.replace(' ', '+')
    town_ = town_.replace(' ', '+')

    final_address = f'https://geocode.maps.co/search?q={address_}+{town_}NJ+{zipcode_}+US&api_key={geocode_api()}'

    json_obj = requests.get(final_address).json()

    if json_obj == []:

        raise ValueError

    else:

        return float(json_obj[0]['lat']), float(json_obj[0]['lon'])


@st.cache_data(ttl=timedelta(days=7))
def query_market_data():

    time_delta = timedelta(days=120)
    start_date = (datetime.now() - time_delta).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    query = (f'''SELECT mlsnum, status_short, address, town, county, zipcode, nj_towncode, month, 
            listprice, olp_lp, salesprice, sp_lp, listdate, closeddate, year_built,
            styleprimary_short, rooms, beds, bathstotal, building_sqft, lotsize_sqft_orig,
            daysonmarket, condition, distressed_sale, taxamount, latitude, 
            longitude, assesstotal, year FROM gsmls_imputed_data 
            WHERE status_short = 'SD' AND closeddate >= '{start_date}' AND closeddate <= '{end_date}';''')

    df = pd.read_sql_query(query, st.session_state['sql_connection'][0])
    df.columns = df.columns.str.upper()
    df.rename(columns={'LATITUDE': 'PROP_LATITUDE', 'LONGITUDE': 'PROP_LONGITUDE',
                       'OLP_LP': 'OLP/LP%', 'SP_LP': 'SP/LP%',
                       'LOTSIZE_SQFT_ORIG': 'LOTSIZE (SQFT)',
                       'BUILDING_SQFT': 'building_sqft', 'YEAR_BUILT': 'year_built'}, inplace=True)

    return df


@st.cache_data(ttl=timedelta(days=30))
def query_ncjar_data():

    current_year = datetime.now().year
    current_month = datetime.now().month

    query = f'''
    SELECT CAST("Date" as date) as Date, "Municipality", "County", "Month", "Year", "New Listings", "Closed Sales", "Days on Markets", "Median Sales Prices",
    "Percent of Listing Price Received", "Inventory of Homes for Sales", "Months of Supply"
    FROM nj_realtor_basic
    WHERE CAST("Date" as date) >= '{current_year}-{current_month - 4}-01';
    '''

    df = pd.read_sql(query, st.session_state['sql_connection'][1])

    # Do data transformations to match columns and datatypes with GSMLS data
    df['date'] = df['date'].astype('string')
    df['Month'] = df['date'].str.split('-').str.get(1)
    df['Month'] = df['Month'].astype('int64')
    df['County'] = df['County'].str.replace(' County', '')
    df.drop(columns='date', inplace=True)
    df = df.rename(columns={'Municipality': 'mun', 'Days on Markets': 'daysonmarket',
                            'Median Sales Prices': 'salesprice', 'Percent of Listing Price Received': 'sp/lp%',
                            'Inventory of Homes for Sales': 'inventory', })
    df.columns = df.columns.str.upper()

    return df


@st.cache_data
def query_state_data():

    query = f'SELECT "Municipality", "County" FROM nj_geographic_data ORDER BY "County", "Municipality";'

    df = pd.read_sql_query(query, st.session_state['sql_connection'][1])

    return df


def query_tax_data(partial_address: str, municipal_code):

    query = f'''
            SELECT yearbuilt, acreage, building_sqft FROM nj_tax_assessor_data
            WHERE municipality = \'{municipal_code}\' AND property_location ILIKE \'{partial_address}%%\'
        '''

    df = pd.read_sql_query(query, st.session_state['sql_connection'][0])

    assert df.empty is not True, 'Tax data was not found'

    df = df.rename(columns={'yearbuilt': 'year_built'})
    # st.session_state['tax_info'] = df.T

    return df.T


def reset():
    st.session_state['get_comps'] = False
    st.session_state['reset'] = True
    st.session_state['prev_address'] = None


def standard_form_1():

    with st.form("Comparables", enter_to_submit=False, clear_on_submit=True):

        # Run program after all inputs are made
        left_, right_ = st.columns(2)
        right_.form_submit_button('Get Comps', type='primary', on_click=get_comps, kwargs={'prev_run': None})
        left_.form_submit_button('Reset', type='secondary', on_click=reset)

        st.markdown(
            ":gray-badge[Required Information]"
        )

        # Address Input
        st.text_input("Address", key='address',
                                value=None, placeholder='123 Main Street',
                                help='Input the address of the property (eg: 123 Main Street)')

        # Use the Dropdown menu to choose the municipality the property is in
        st.selectbox("Municipality", options=st.session_state['municipality_list'],
                            key='municipality')

        st.text_input("Zipcode", key='zipcode')

        st.date_input("Listed Date", value=st.session_state['listdate'], format="YYYY-MM-DD",
                                 key='listdate', max_value=str(datetime.today()),
                                 help='Input the date the property was first listed '
                                      'or earliest date of availability')

        # Input the listing price
        st.number_input("Listing Price", value="min", placeholder='$350,000',
                                    key='listprice', min_value=50000)

        # Use the Dropdown menu to choose the county the property is in
        st.selectbox("Primary Style of Property",
                                  options=st.session_state['primary_style_list'],
                                  key='primary_style')

        st.selectbox("Total Rooms", options=[i for i in range(20)],
                             key='rooms', placeholder='1', help='Input the total amount of rooms in a property. '
                                                                'This usually includes bathrooms, bedrooms, '
                                                                'kitchens and living rooms (Required)')

        # Input number of bedrooms
        st.selectbox("Bedrooms", options=[i for i in range(9)],
                                key='bedrooms', placeholder='1', help='Input the number of bedrooms available in'
                                                                      ' the property (Required)')

        # Input number of bedrooms
        st.number_input("Bathrooms", value="min", min_value=1.0, key='bathrooms',
                                    placeholder='1.0', format="%0.1f",
                                    help="Half bathrooms are represented by 0.1. For example, if there are 2 full"
                                         "bathrooms and one half bathroom, you'd insert '2.1 (Required)'")

        st.selectbox("Fireplaces", options=[i for i in range(4)],
                                  placeholder='0', key='fireplaces',
                                  help='Input the number of fireplaces in the property (Required)')

        st.selectbox("Garage Capacity", placeholder='0', options=[i for i in range(9)],
                                 key='garagecap', help='Input how many cars can fit '
                                                       'in the garage if one is available (Required)')

        st.number_input("Taxes (Previous Year)", placeholder='1000',
                                key='taxes', help="Input the previous full year taxes (Required)")

        st.divider()

        st.markdown(
            ":gray-badge[Information Not Required]"
        )

        st.number_input("Build Year", min_value=1680,
                                     value="min", placeholder='1680', key='year_built',
                                     help='Input the year that the property was built (Not required)')

        st.number_input("Total Assessment Value", min_value=0,
                                       value="min", placeholder='100000', key='assesstotal',
                                       help="Input the previous year's total assessment value for the property"
                                            " (Not required)")

        st.number_input("Lot Size (Sqft)", min_value=0,
                                   value="min", placeholder='5000', key='lot_size',
                                   help='Input the Lot Size in Square Feet (Not required)')

        st.number_input("Liveable Area (Sqft)", placeholder='1000', key='sq_ft',
                                help="Input the Gross Liveable Area (GLA) better known as the interior sqft"
                                     " that an owner is able to utilize. Basement sqft isn't considered liveable"
                                     " even if it's finished or made into a bedroom/living room (Not required)")

        st.selectbox("Property Condition",
                                 options=['Poor', 'Fair', 'Average', 'Good', 'Excellent'],
                                 key='condition', placeholder='Average')

        st.caption("While this information isn't required, it is strongly suggested to fill out if it "
                 "is known. Otherwise, this data will be estimated and lead to less accurate results.")


def standard_form_2():
    with st.form("Comparables", enter_to_submit=False, clear_on_submit=True):

        # Run program after all inputs are made
        left_, right_ = st.columns(2)
        right_.form_submit_button('Get Comps', type='primary', on_click=get_comps, kwargs={'prev_run': None})
        left_.form_submit_button('Reset', type='secondary', on_click=reset)

        st.markdown(
            ":gray-badge[Required Information]"
        )

        # Address Input
        st.text_input("Address", key='address',
                                value=None, placeholder='123 Main Street',
                                help='Input the address of the property (eg: 123 Main Street)')

        # Use the Dropdown menu to choose the municipality the property is in
        st.selectbox("Municipality", options=st.session_state['municipality_list'],
                            key='municipality')

        st.text_input("Zipcode", key='zipcode')

        st.date_input("Listed Date", value=st.session_state['listdate'], format="YYYY-MM-DD",
                                 key='listdate', max_value=str(datetime.today()),
                                 help='Input the date the property was first listed '
                                      'or earliest date of availability')

        # Input the listing price
        listprice = st.number_input("Current Listing Price", value="min", placeholder='$350,000',
                                    key='listprice', min_value=50000)

        og_listprice = st.number_input("Original listing price", value="min", placeholder='$350,000',
                                       min_value=50000, help='Input the listing price when the property was initially'
                                                             ' listed and before the price was increased/reduced')

        pct_val = ((listprice - og_listprice) / og_listprice) * 100
        st.number_input("OLP/LP%", value=pct_val, placeholder='0.0', key='olp/lp%', disabled=True,
                        min_value=-100.00, help='Percentage change from the original listing price')

        # Use the Dropdown menu to choose the county the property is in
        st.selectbox("Primary Style of Property",
                                  options=st.session_state['primary_style_list'],
                                  key='primary_style')

        st.selectbox("Total Rooms", options=[i for i in range(20)],
                             key='rooms', placeholder='1', help='Input the total amount of rooms in a property. '
                                                                'This usually includes bathrooms, bedrooms, '
                                                                'kitchens and living rooms (Required)')

        # Input number of bedrooms
        st.selectbox("Bedrooms", options=[i for i in range(9)],
                                key='bedrooms', placeholder='1', help='Input the number of bedrooms available in'
                                                                      ' the property (Required)')

        # Input number of bedrooms
        st.number_input("Bathrooms", value="min", min_value=1.0, key='bathrooms',
                                    placeholder='1.0', format="%0.1f",
                                    help="Half bathrooms are represented by 0.1. For example, if there are 2 full"
                                         "bathrooms and one half bathroom, you'd insert '2.1 (Required)'")

        st.selectbox("Fireplaces", options=[i for i in range(4)],
                                  placeholder='0', key='fireplaces',
                                  help='Input the number of fireplaces in the property (Required)')

        st.selectbox("Garage Capacity", placeholder='0', options=[i for i in range(9)],
                                 key='garagecap', help='Input how many cars can fit '
                                                       'in the garage if one is available (Required)')

        st.number_input("Taxes (Previous Year)", placeholder='1000',
                                key='taxes', help="Input the previous full year taxes (Required)")

        st.divider()

        st.markdown(
            ":gray-badge[Information Not Required]"
        )

        st.number_input("Build Year", min_value=1680,
                                     value="min", placeholder='1680', key='year_built',
                                     help='Input the year that the property was built (Not required)')

        st.number_input("Total Assessment Value", min_value=0,
                                       value="min", placeholder='100000', key='assesstotal',
                                       help="Input the previous year's total assessment value for the property"
                                            " (Not required)")

        st.number_input("Lot Size (Sqft)", min_value=0,
                                   value="min", placeholder='5000', key='lot_size',
                                   help='Input the Lot Size in Square Feet (Not required)')

        st.number_input("Liveable Area (Sqft)", placeholder='1000', key='sq_ft',
                                help="Input the Gross Liveable Area (GLA) better known as the interior sqft"
                                     " that an owner is able to utilize. Basement sqft isn't considered liveable"
                                     " even if it's finished or made into a bedroom/living room (Not required)")

        st.selectbox("Property Condition",
                                 options=['Poor', 'Fair', 'Average', 'Good', 'Excellent'],
                                 key='condition', placeholder='Average')

        st.caption("While this information isn't required, it is strongly suggested to fill out if it "
                 "is known. Otherwise, this data will be estimated and lead to less accurate results.")


st.set_page_config(layout='wide')

# Initialize all variables which need to start the session
with st.status("Querying Data...") as status:
    if 'sql_connection' not in st.session_state:
        status.update(label='Creating database connections...', expanded=True, state='running')
        start_time = time.time()
        st.session_state['sql_connection'] = [njtax_connection(), njrealtor_connection()]
        end_time = time.time()
        st.write(f'Database connections created! Time elapsed: {round(end_time - start_time, 2)}')

    if 'market_data' not in st.session_state:
        status.update(label='Querying real estate data...', expanded=True, state='running')
        start_time = time.time()
        st.session_state['start_year'] = ''
        st.session_state['end_year'] = ''
        st.session_state['start_month'] = ''
        st.session_state['end_month'] = ''
        st.session_state['market_data'] = query_market_data()
        st.session_state['primary_style_list'] = primary_style_list()
        st.session_state['aggregate_data'], st.session_state['mean_sqft'] = create_agg_data()
        st.session_state['cur_mortgage_rate'], st.session_state['prev_mortgage_rate'] = get_mortgage_rates()
        st.session_state['comp_data'] = None
        end_time = time.time()
        st.write(f'Real estate data acquired! Time elapsed: {round(end_time - start_time, 2)}')

    if 'state_data' not in st.session_state:
        status.update(label='Querying Geojson data...', expanded=True, state='running')
        start_time = time.time()
        st.session_state['state_data'] = query_state_data()
        st.session_state['ncjar_data'] = query_ncjar_data()
        st.session_state['geojson'] = load_nj_geojson()
        st.session_state['choropleth_data'] = create_choropleth_data()
        st.session_state['county_list'] = st.session_state['state_data']['County'].unique().tolist()
        st.session_state['county'] = None
        st.session_state['municipality_list'] = None
        st.session_state['municipality'] = None
        end_time = time.time()
        st.write(f'Geojson data acquired! Time elapsed: {round(end_time - start_time, 2)}')

    if 'default_map' not in st.session_state:
        status.update(label='Creating state choropleth map...', expanded=True, state='running')
        start_time = time.time()
        st.session_state['default_map'] = default_map()
        st.session_state['agg_map'] = create_choropleth(st.session_state['default_map'])
        st.session_state['comp_map'] = None
        st.session_state['reset'] = True
        st.session_state['get_comps'] = False
        end_time = time.time()
        st.write(f'Choropleth map created! Time elapsed: {round(end_time - start_time, 2)}')

    if 'cls_model' not in st.session_state:
        status.update(label='Accessing ML algorithms...', expanded=True, state='running')
        start_time = time.time()
        st.session_state['cls_model'] = load_cls_model()
        st.session_state['reg_model'] = load_reg_model()
        st.session_state['transform_pipeline'] = load_transform_pipeline()
        st.session_state['targ_price_pipeline'] = load_targ_price_pipeline()
        end_time = time.time()
        st.write(f'ML algorithms accessed! Time elapsed: {round(end_time - start_time, 2)}')

    if 'listprice' not in st.session_state:
        status.update(label='Creating session state variables...', expanded=True, state='running')
        start_time = time.time()
        todays_date = datetime.today()
        year = todays_date.year
        month = todays_date.month
        day = todays_date.day

        st.session_state['previously_run'] = defaultdict(lambda: "Not Present")
        st.session_state['month_str'] = todays_date.strftime("%Y-%B-%d").split('-')[1]
        st.session_state['target_property'] = None
        st.session_state['address'] = None
        st.session_state['zipcode'] = '00000'
        st.session_state['listdate'] = date(year, month, day)
        st.session_state['listprice'] = 50000
        st.session_state['olp/lp%'] = 0.0
        st.session_state['primary_style'] = None
        st.session_state['bedrooms'] = 0
        st.session_state['bathrooms'] = 1.0
        st.session_state['rooms'] = 0
        st.session_state['fireplaces'] = 0
        st.session_state['garagecap'] = 0
        st.session_state['condition'] = None
        st.session_state['prev_address'] = None
        # Variables not readily known
        st.session_state['year_built'] = 1680
        st.session_state['lot_size'] = 0
        st.session_state['daysonmarket'] = 0
        st.session_state['sq_ft'] = 0
        st.session_state['taxes'] = 0
        st.session_state['assesstotal'] = 0
        end_time = time.time()
        st.write(f'Variables created! Time elapsed: {round(end_time - start_time, 2)}')
        # Used for debugging
        # st.session_state['municipal_info'] = ''
        # st.session_state['tax_info'] = ''
    status.update(label='Process completed!', expanded=False, state='complete')

tab1, tab2, tab3 = st.tabs(['Dashboard', 'About', 'Tooltips & Help'])

with tab1:

    with st.sidebar:

        st.title("NJ Comps Dashboard")

        # Will hold a list of previously run comps. Clicking one from the list will auto-populate the value cells
        if len(list(st.session_state['previously_run'].keys())) == 0:
            # The default value of '-' shouldn't be selectable
            # This is only a placeholder value when resetting the data
            previous_comps = st.selectbox("Previously run comps", options=[],
                                          key='prev_address', on_change=get_comps, kwargs={'prev_run': 'Yes'})
        else:
            previous_comps = st.selectbox("Previously run comps", options=list(st.session_state['previously_run'].keys()),
                                          index=None, placeholder='Choose a property...',
                                          key='prev_address', on_change=get_comps, kwargs={'prev_run': 'Yes'})

        # Use the Dropdown menu to choose the county the property is in
        county = st.selectbox("Choose County", options=st.session_state['county_list'],
                              key='county', on_change=create_municipality_list)

        # Ask if the listing price has been reduced/increased since being listed
        # If it has, get the original listing price
        original_listprice = st.toggle("Has the listing price been reduced/increased since being listed?", value=False)

        if original_listprice is True:

            standard_form_2()

        else:

            standard_form_1()

    with st.container():

        if st.session_state['reset'] is True:

            left, middle1, middle2, right = st.columns(4)

            comps = st.session_state['ncjar_data']
            current_data = comps[(comps['YEAR'] == comps['YEAR'].max()) & (comps['MONTH'] == comps['MONTH'].max())]
            if current_data['MONTH'].max() == 1:
                prev_data = comps[(comps['YEAR'] == comps['YEAR'].min()) & (comps['MONTH'] == comps['MONTH'].max())]
            else:
                prev_data = comps[
                    (comps['YEAR'] == comps['YEAR'].max()) & (comps['MONTH'] == comps['MONTH'].max() - 1)]

            current_splp = current_data['SP/LP%'] * 100
            prev_splp = prev_data['SP/LP%'] * 100
            current_avg_splp = round(current_splp.mean(), 2)
            prev_avg_splp = round(prev_splp.mean(), 2)
            splp_diff = round(current_avg_splp - prev_avg_splp, 2)
            current_avg_sp = round(current_data['SALESPRICE'].quantile(0.50), 2)
            prev_avg_sp = round(prev_data['SALESPRICE'].quantile(0.50), 2)
            sp_diff = round(current_avg_sp - prev_avg_sp, 2)
            curennt_avg_sp = price_formatter(str(current_avg_sp))
            current_sold_props = current_data['CLOSED SALES'].sum()
            prev_sold_props = prev_data['CLOSED SALES'].sum()
            sold_diff = int(current_sold_props - prev_sold_props)

            with left:
                st.metric(label="Average SP/LP% in NJ",
                          value=f'{current_avg_splp}%', delta=splp_diff, border=True)

            with middle1:
                st.metric(label="NJ Median Sales Price",
                          value=f'${current_avg_sp}', delta=f'${price_formatter(str(sp_diff))}', border=True)

            with middle2:
                st.metric(label="Number of Sold Homes",
                          value=current_sold_props, delta=sold_diff, border=True)

            with right:
                diff = round(st.session_state['cur_mortgage_rate'] - st.session_state['prev_mortgage_rate'], 2)
                st.metric(label=f"Mortgage Rate (as of {datetime.now().date()})",
                          value=f"{st.session_state['cur_mortgage_rate']}%", delta=diff, delta_color='inverse',
                          border=True)

        elif st.session_state['get_comps'] is True:

            left, middle1, middle2, middle3, right = st.columns(5)

            comps = st.session_state['aggregate_data']
            current_data = comps[(comps['YEAR'] == comps['YEAR'].max())
                                 & (comps['MONTH'] == comps['MONTH'].max())
                                 & (comps['MUN'] == st.session_state['municipality'])]
            if current_data['MONTH'].max() == 1:
                prev_data = comps[(comps['YEAR'] == comps['YEAR'].min())
                                  & (comps['MONTH'] == comps['MONTH'].max())
                                  & (comps['NUN'] == st.session_state['municipality'])]
            else:
                prev_data = comps[
                    (comps['YEAR'] == comps['YEAR'].max()) & (comps['MONTH'] == comps['MONTH'].max() - 1)
                    & (comps['MUN'] == st.session_state['municipality'])]

            current_splp = current_data['SP/LP%'] + 100
            prev_splp = prev_data['SP/LP%'] + 100
            current_avg_splp = round(current_splp.mean(), 2)
            prev_avg_splp = round(prev_splp.mean(), 2)
            splp_diff = round(current_avg_splp - prev_avg_splp, 2)
            current_avg_sp = round(current_data['SALESPRICE'].quantile(0.50), 2)
            prev_avg_sp = round(prev_data['SALESPRICE'].quantile(0.50), 2)
            sp_diff = round(current_avg_sp - prev_avg_sp, 2)
            curennt_avg_sp = price_formatter(str(current_avg_sp))
            current_sold_props = current_data['MLSNUM'].values[0]
            prev_sold_props = prev_data['MLSNUM'].values[0]
            sold_diff = int(current_sold_props - prev_sold_props)

            with left:
                if st.session_state['address'] is not None and st.session_state['prev_address'] is None:
                    cls_pred = st.session_state['target_property']['cls_prediction'][0]
                    st.metric(label=f"Likelihood Sold*",
                              value=f"{round(cls_pred * 100, 2)}%", border=True)
                else:
                    cls_pred = st.session_state['previously_run'][st.session_state['prev_address']]['cls_prediction'][0]
                    st.metric(label=f"Likelihood Sold*",
                              value=f"{round(cls_pred * 100, 2)}%", border=True)

            with middle1:
                if st.session_state['address'] is not None and st.session_state['prev_address'] is None:
                    pred_price = st.session_state['target_property']['reg_prediction'][0]
                    pred_price_str = price_formatter(str(st.session_state['target_property']['reg_prediction'][0]))

                    st.metric(label=f"SP for {st.session_state['address']}*", delta_color='off',
                              value=f'${pred_price_str}',
                              delta=f'+/- ${price_formatter(str(round(pred_price * 0.05, 2)))}',
                              border=True)
                else:
                    pred_price = st.session_state['previously_run'][st.session_state['prev_address']]['reg_prediction'][0]
                    pred_price_str = price_formatter(
                        str(st.session_state['previously_run'][st.session_state['prev_address']]['reg_prediction'][0]))

                    st.metric(label=f"SP for {st.session_state['prev_address']}*", delta_color='off',
                              value=f'${pred_price_str}',
                              delta=f'+/- ${price_formatter(str(round(pred_price * 0.05, 2)))}',
                              border=True)

            with middle2:
                if sp_diff < 1.0:
                    # For a negative difference in Median Sales Price, I need to make the number negative
                    st.metric(label=f"{st.session_state['month_str']} Median SP in {st.session_state['municipality']}",
                              value=f'${price_formatter(str(current_avg_sp))}',
                              delta=f'-${price_formatter(str(abs(sp_diff)))}', border=True)
                else:
                    st.metric(label=f"{st.session_state['month_str']} Median SP in {st.session_state['municipality']}",
                              value=f'${price_formatter(str(current_avg_sp))}',
                              delta=f'${price_formatter(str(sp_diff))}', border=True)

            with middle3:
                st.metric(label=f"{st.session_state['month_str']} Avg SP/LP% in {st.session_state['municipality']}",
                          value=f'{current_avg_splp}%', delta=splp_diff, border=True)

            with right:
                st.metric(label=f"{st.session_state['month_str']} Sold Homes in {st.session_state['municipality']}",
                          value=current_sold_props, delta=sold_diff, border=True)

            if st.session_state['address'] is not None and st.session_state['prev_address'] is None:

                likelihood_factors()

                pros_remarks = st.session_state['previously_run'][st.session_state['address']]['pro_remarks'][0]
                cons_remarks = st.session_state['previously_run'][st.session_state['address']]['con_remarks'][0]

                if cls_pred < 0.50:
                    if cons_remarks != '':
                        st.error(f"There's a high likelihood {st.session_state['address']} won't sell. "
                                 f"Here are some possible reasons why:"
                                 f"\n - {cons_remarks}")
                    if pros_remarks != '':
                        st.info(f"Here are some positive characteristics about {st.session_state['address']}:"
                                f"\n - {pros_remarks}")
                else:
                    if pros_remarks != '':
                        st.info(f"Here are some positive characteristics about {st.session_state['address']}:"
                                f"\n - {pros_remarks}")
                    if cons_remarks != '':
                        st.error(f"Here are some characteristics about {st.session_state['address']} that may make"
                                 f" other properties attractive:"
                                 f"\n - {cons_remarks}")

            else:
                pros_remarks = st.session_state['previously_run'][st.session_state['prev_address']]['pro_remarks'][0]
                cons_remarks = st.session_state['previously_run'][st.session_state['prev_address']]['con_remarks'][0]

                if cls_pred < 0.50:
                    if cons_remarks != '':
                        st.error(f"There's a high likelihood {st.session_state['prev_address']} won't sell. "
                                 f"Here are some possible reasons why..."
                                 f"\n - {cons_remarks}")
                    if pros_remarks != '':
                        st.info(f"Here are some positive characteristics about {st.session_state['prev_address']}"
                                f"\n - {pros_remarks}")
                else:
                    if pros_remarks != '':
                        st.info(f"Here are some positive characteristics about {st.session_state['prev_address']}:"
                                f"\n - {pros_remarks}")
                    if cons_remarks != '':
                        st.error(f"Here are some characteristics about {st.session_state['prev_address']} that may make"
                                 f" other properties attractive:"
                                 f"\n - {cons_remarks}")

    with st.container():

        if st.session_state['reset'] is True:
            folium_static(st.session_state['agg_map'], width=2400, height=600)

            with st.expander('Comparable Legend', expanded=True):

                st.markdown(
                    ":violet-badge[:material/radio_button_checked: Data Not Available]"
                )

        elif st.session_state['get_comps'] is True:
            folium_static(st.session_state['comp_map'], width=2400, height=500)

            with st.expander('Legend', expanded=True):

                st.markdown(
                    ":blue-badge[:material/radio_button_checked: Best Comp] \t :orange-badge[:material/radio_button_checked: Average Comp] \t :red-badge[:material/radio_button_checked: Below Average Comp]"
                )

        st.caption(
            """
            Disclaimer:
            This real estate dashboard uses artificial intelligence (AI) to analyze and present publicly available 
            property data. The values, insights, or recommendations generated by the AI are for informational purposes 
            only and should not be considered accurate, current, or a substitute for professional advice or appraisal 
            services.

            By using this site, you acknowledge and agree that:
            The accuracy, adequacy, quality, currentness, validity, completeness, or suitability of any data for any 
            purpose is not guaranteed.
            The functions or services of this website may be interrupted or contain errors.
            You assume full responsibility for any decisions made based on the information presented, and the website 
            owner shall not be held legally liable for any loss or damages resulting from its use.
            All information presented on this site is sourced from public records and datasets and is provided in 
            compliance with Daniel’s Law (N.J.S.A. 47:1-17 et seq.) regarding the protection of personal information of 
            certain individuals in the State of New Jersey.
            Use of this website constitutes acceptance of this disclaimer.
            """
        )

with tab2:

    with st.container():
        st.write("""
                Welcome to the NJ Comparables Dashboard.  A one-stop shop for real estate agents, home buyers and home 
                sellers alike in the state of NJ to generate quick comparable properties as well as gauge a subject 
                property's sales price based on its physical and locational characteristics. In addition, one can 
                assess local real estate market activity with closed homes (metrics such as homes available, currently 
                under contract, withdrawn/expired coming soon) to help recognize what transactions are occurring on a 
                weekly basis. This dashboard enlists the help of machine learning algorithms 
                to approximate the likelihood of a home being sold and how much it will sell for based on the 
                characteristics of the property provided. The regression algorithm has achieved a coefficient of 
                determination (r2 Score) of 96.8% and mean absolute percentage error (MAPE) of 5.1% while the 
                classification algorithm has achieved a f1 Score of 85%. Efforts are being made daily to increase 
                their capabilities and improve results. 

        """)


with tab3:

    with st.container():
        st.write("""

        Definitions:

        - Average Comp:   A property which have some similar characteristics as the subject property. At minimum, 
                      the home will be the same home type as the subject property but may differ in similar 
                      liveable sqft, bedrooms, bathrooms, and or total rooms

        - Below Average Comp:   A property which does not have any similar characteristics as the subject property.

        - Best Comp: A property which have extremely similar characteristics as the subject property such as similar 
                    sqft, bedrooms, bathrooms, total rooms and home type (Colonial, Cape Code, Victorian, etc).

        - Coefficient of determination: In statistics, the coefficient of determination, denoted R2 or r2 and pronounced 
                           "R squared", is the proportion of the variation in the dependent variable that is predictable 
                           from the independent variable(s). It is a statistic used in the context of statistical models 
                           whose main purpose is either the prediction of future outcomes or the testing of hypotheses, 
                           on the basis of other related information. It provides a measure of how well observed 
                           outcomes are replicated by the model, based on the proportion of total variation of outcomes 
                           explained by the model. The lowest possible value of R² is 0 and the highest possible value 
                           is 1. Put simply, the better a model is at making predictions, 
                           the closer its R² will be to 1.

        - Comparable (Comp):   is a real estate appraisal term referring to properties with physical and locational 
                           characteristics that are similar to a subject property whose value is being sought. 
                           This can be accomplished either by a real estate agent who attempts to establish the value 
                           of a potential client's home or property through market analysis or, by a licensed or 
                           certified appraiser or surveyor using more defined methods, when performing a real estate 
                           appraisal.

        - F1 - Score: The F1 score is the harmonic mean of the precision and recall. It thus symmetrically represents 
                      both precision and recall in one metric. The highest possible value of an F-score is 1.0, 
                      indicating perfect precision and recall, and the lowest possible value is 0, if the precision or 
                      the recall is zero.

        - Mean Absolute Percentage Error: The mean absolute percentage error (MAPE), is a measure of prediction accuracy 
                                of a forecasting method in statistics. MAPE is commonly used because it’s easy to 
                                interpret. For example, a MAPE value of 5% means that the average difference between 
                                the forecasted value and the actual value is 5%.

        - Precision: Precision (also called positive predictive value) is the fraction of positively identified 
                     instances among the all instances identified (true positives and false positives) for one class. 
                     Precision answers the question: Of all the instances the model predicted as positive, 
                     how many were actually correct?

        - Recall: Recall (also known as sensitivity) is the fraction of positively identified instances among all
                  relevant instances (true positives and false negatives) for one class. Recall answers the question: 
                  Of all the actual positive instances, how many did the model correctly identify? 

        - Sale Price to Listing Price Ratio (SP/LP):   A ratio describing how much of the listing price is  the sellers
                                                   of sold properties are receiving. A ratio of 100% means the sellers
                                                   are selling for the exact price they're asking for. A ratio over 100%
                                                   means they're getting more than the listing price, while a ratio 
                                                   under 100% means they are getting less. For example, a property 
                                                   receiving a SP/LP of 106% mean it sold for 6% more than the listing
                                                   price. A SP/LP of 87% means the home sold for 87% (13% less) 
                                                   of the listing price.

        """)


st.write(st.session_state)
