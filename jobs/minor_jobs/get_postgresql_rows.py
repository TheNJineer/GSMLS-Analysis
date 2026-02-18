import argparse
import sys
import pandas as pd
from gsmls.utility_func import create_sql_engine, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Query amount of data available in PostgresSQL table')
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--key", required=True)

    # return parser.parse_args(['--table_name', 'res_properties', "--key", "postgresql_start"])
    return parser.parse_args()


def get_postgresql_rows(table_name_, remote=True):

    engine = create_sql_engine("gsmls", remote=remote)

    query = f"SELECT COUNT(*) FROM {table_name_};"

    # The version discrepancy between Pandas 2.x and SQLAlchemy 1.4.x forces
    # the user to create a raw DBAPI connection which Pandas expects
    # Throws AttributeError "Engine/Connection object has no .cursor() method"
    df = pd.read_sql_query(query, con=engine)
    print(f' ==== TOTAL ROW COUNT FROM POSTGRESQL ACQUIRED: {int(df.loc[0].values[0])} ====  ')

    return {'table_name': table_name_, 'prop_count': int(df.loc[0].values[0])}


if __name__ == '__main__':

    args = parse_args()
    status = get_postgresql_rows(args.table_name)
    check_pipeline_metadata("gsmls_airflow_pipeline", key=args.key, status=status)
    sys.exit(0)

