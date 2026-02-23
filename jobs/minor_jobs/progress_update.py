import pandas as pd
import argparse
import os
import shelve
import sys
from pprint import pprint
from gsmls.utility_func import create_sql_engine, create_mongodb_conn
from gsmls.utility_func import get_filepath, check_pipeline_metadata


def create_message(prop_type: str, tracker: dict):

    message = f"""
                <br>
                <b>Program Progress</b><br>
                <b>Current Year</b>: {tracker['year']}<br>
                <b>Last Scraped Municipality</b>: {tracker['municipality']}<br>
                <b>County</b>: {tracker['county']}<br>
                <b>Finished</b>: {tracker['finished']}<br>
                <b>Data Produced</b>: {tracker['rows_produced']} Rows<br>
                <b>MongoDB Document Count</b>: {tracker['documents']}<br>
                <b>Property Type</b>: {prop_type}<br<br>
                """

    return message


def create_progress_tracker():

    progress_tracker = {
        'year': None,
        'municipality': None,
        'county': None,
        'rows_produced': None,
        'documents': None,
        'split_type': None,
        'split_index': None,
        'finished': None
    }

    print(f' ==== CREATING NEW PROGRESS CONTAINER ==== ')
    return progress_tracker


def current_status(prop_type):

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    print(f' ==== CHECKING CURRENT STATUS OF GSMLS PIPELINE ==== ')
    with shelve.open(metadata_path) as reader:
        tracker = reader["gsmls_airflow_pipeline"][prop_type]["progress_tracker"]
        message = reader["gsmls_airflow_pipeline"][prop_type]["progress_message"]

    print(f' ==== STATUS OF GSMLS PIPELINE ACQUIRED ==== ')
    pprint(tracker)
    return tracker, message


def parse_args():

    parser = argparse.ArgumentParser(description='Check the GSMLS data start point for target property type')
    parser.add_argument("--prop_type", required=True)
    parser.add_argument("--pipeline", required=True)

    # return parser.parse_args(['--prop_type', 'RES', '--pipeline', "gsmls_airflow_pipeline"])
    return parser.parse_args()


def progress_update(prop_type, pipeline):

    # Create database connections and pull preliminary data
    engine = create_sql_engine("gsmls", remote=True)
    client = create_mongodb_conn(remote=True)
    database = client['realEstate']
    num_of_docs_ = database['propertyImages'].count_documents({})

    prev_tracker, prev_message = current_status(prop_type)
    tracker = create_progress_tracker()
    tracker["documents"] = num_of_docs_
    df = query_property_data(prop_type, engine)
    tracker = update_tracker(df, tracker)
    message = create_message(prop_type, tracker)

    # If the date of the data produced and today doesn't match, no data has been scraped today
    if tracker["municipality"] is not None and prev_tracker is None:
        print(' ==== NEW STATUS TRACKER CREATED ==== ')
        check_pipeline_metadata(pipeline, prop_type_=prop_type, key_="progress_tracker", status_=tracker)
        check_pipeline_metadata(pipeline, prop_type_=prop_type, key_="progress_message", status_=message)
        print(f' ==== GSMLS PIPELINE STATUS HAS BEEN UPDATED ==== ')
    elif (tracker["municipality"] != prev_tracker["municipality"]) and (tracker["county"] != prev_tracker["county"]):
        check_pipeline_metadata(pipeline, prop_type_=prop_type, key_="progress_tracker", status_=tracker)
        check_pipeline_metadata(pipeline, prop_type_=prop_type, key_="progress_message", status_=message)
        print(f' ==== GSMLS PIPELINE STATUS HAS BEEN UPDATED ==== ')


def query_property_data(prop_type, engine):

    print(f' ==== ACQUIRING LATEST DATA FROM GSMLS EVENT LOG  ==== ')
    try:
        # Scrape the event log for the latest saved event which occurred
        query = f"""
                    SELECT * FROM gsmls_event_log_new
                    WHERE id = (SELECT MAX(id) FROM gsmls_event_log_new)
                """
        df = pd.read_sql_query(query, con=engine)
        last_row = df.shape[0] - 1
        property_type = df.loc[last_row, "property_type"]

        assert prop_type == property_type

    except AssertionError:
        query = f"""
                SELECT * FROM gsmls_event_log_new
                WHERE property_type = '{prop_type}'
                ORDER BY id DESC
                LIMIT 1;
            """
        df = pd.read_sql_query(query, con=engine.raw_connection())

    return df


def update_tracker(data: pd.DataFrame, tracker: dict):

    last_row = data.shape[0] - 1

    for key in tracker.keys():
        try:
            if key == 'year':
                tracker[key] = data.loc[last_row, "year_"]
            else:
                tracker[key] = data.loc[last_row, key]
        except KeyError:
            pass

    print(f' ==== PROGRESS TRACKER UPDATED WITH MOST RECENT INFORAMTION ==== ')
    pprint(tracker)
    return tracker


if __name__ == '__main__':

    args = parse_args()
    progress_update(args.prop_type, args.pipeline)
    sys.exit(0)

