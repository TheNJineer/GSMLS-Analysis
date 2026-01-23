import pandas as pd
import argparse
import os
import shelve
import sys
from gsmls_core.gsmls.utility_func import create_sql_engine, create_mongodb_conn
from gsmls_core.gsmls.utility_func import get_filepath, check_pipeline_metadata


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

    return progress_tracker


def current_status():

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        tracker = reader["gsmls_airflow_pipeline"]["progress_tracker"]
        message = reader["gsmls_airflow_pipeline"]["progress_message"]

    return tracker, message


def parse_args():

    parser = argparse.ArgumentParser(description='Check the GSMLS data start point for target property type')
    parser.add_argument("--prop_type", required=True)
    parser.add_argument("--pipeline", required=True)

    return parser.parse_args(['--prop_type', 'RES', '--pipeline', "gsmls_airflow_pipeline"])


def progress_update(prop_type, pipeline):

    # Create database connections and pull preliminary data
    engine = create_sql_engine("gsmls", remote=True)
    client = create_mongodb_conn(remote=True)
    database = client['realEstate']
    num_of_docs_ = database['propertyImages'].count_documents({})

    prev_tracker, prev_message = current_status()
    tracker = create_progress_tracker()
    tracker["documents"] = num_of_docs_
    df = query_property_data(prop_type, engine)
    tracker = update_tracker(df, tracker)
    message = create_message(prop_type, tracker)

    # If the date of the data produced and today doesn't match, no data has been scraped today
    if tracker["municipality"] is not None and prev_tracker is None:
        check_pipeline_metadata(pipeline, key="progress_tracker", status=tracker)
        check_pipeline_metadata(pipeline, key="progress_message", status=message)
    elif (tracker["municipality"] != prev_tracker["municipality"]) and (tracker["county"] != prev_tracker["county"]):
        check_pipeline_metadata(pipeline, key="progress_tracker", status=tracker)
        check_pipeline_metadata(pipeline, key="progress_message", status=message)


def query_property_data(prop_type, engine):

    try:
        # Scrape the event log for the latest saved event which occurred
        query = f"""
                    SELECT * FROM gsmls_event_log_new
                    WHERE id = (SELECT MAX(id) FROM gsmls_event_log_new)
                """
        df = pd.read_sql_query(query, con=engine.raw_connection())
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

    return tracker


if __name__ == '__main__':

    args = parse_args()
    progress_update(args.prop_type, args.pipeline)
    sys.exit(0)

