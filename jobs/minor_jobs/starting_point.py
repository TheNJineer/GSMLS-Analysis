import argparse
import sys
import pandas as pd
from datetime import datetime
from gsmls.utility_func import create_sql_engine, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Check the GSMLS data start point for target property type')
    parser.add_argument("--prop_type", required=True)

    return parser.parse_args(['--prop_type', 'RES'])


def starting_point(prop_type):

    engine = create_sql_engine("gsmls", remote=True)

    query = f"""
                SELECT * FROM gsmls_event_log_new
                WHERE property_type = '{prop_type}'
                ORDER BY id DESC
                LIMIT 1;
                """

    print(f' ==== QUERYING {prop_type} DATA ==== ')
    metadata = pd.read_sql_query(query, con=engine)
    last_row = metadata.shape[0] - 1

    if metadata.empty:
        print(f" ==== DATA DOESN'T EXIST FOR {prop_type}. BEGINNING DATA SCRAPE ==== ")
        return True

    else:
        last_scraped_county = metadata.loc[last_row, "county"]
        last_scraped_muni = metadata.loc[last_row, "municipality"]
        finished = metadata.loc[last_row, "finished"]
        date_produced = metadata.loc[last_row, "date_produced"]
        timeframe = metadata.loc[last_row, "timeframe"]
        delta = datetime.now() - date_produced
        print(f' ==== TIME DELTA BETWEEN LAST SCRAPE: {delta.days}')

        # All data for the last property type was acquired
        if last_scraped_county == 30 and last_scraped_muni == "White Twp." and finished == "Yes":

            if timeframe in ["historic", "mixed"]:
                print(f' ==== BEGINNING {prop_type} SCRAPE FROM HISTORIC/MIXED TIMEFRAME ==== ')
                return True
            elif delta.days <= 6:
                print(f' ==== NO NEW DATA AVAILABLE FOR {prop_type} ==== ')
                return False
            else:
                print(f' ==== NEW DATA AVAILABLE. BEGINNING {prop_type} SCRAPE ==== ')
                return True

        else:
            print(f' ==== BEGINNING {prop_type} SCRAPE FROM PREVIOUS POINT ==== ')
            return True


if __name__ == '__main__':

    args = parse_args()
    status = starting_point(args.prop_type)
    check_pipeline_metadata("gsmls_airflow_pipeline", key="prop_type", status=args.prop_type)
    check_pipeline_metadata("gsmls_airflow_pipeline", key="start_point", status=status)
    sys.exit(0)

