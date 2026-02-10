import sys
import argparse
import time
from pymongo.errors import ConnectionFailure
from gsmls.utility_func import create_mongodb_conn, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description="Check MongoDB database for available collections")
    parser.add_argument("--db_name", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--key", required=True)

    # return parser.parse_args(['--db_name', 'realEstate',
    #                           "--table_name", "propertyImages",
    #                           "--key", "mongodb_start"])
    return parser.parse_args()


def check_mongodb(db_name, table_name):

    retries = 0

    client = create_mongodb_conn(remote=True)

    # Confirm connection to MongoDB Atlas
    while retries < 10:

        try:
            client.admin.command({"ping": 1})
        except ConnectionFailure as cf:
            print(f"{cf}")
            time.sleep(1)
            retries += 1
        else:
            print(' ==== MONGODB CONNECTION SUCCESSFUL ==== ')
            break

    else:
        raise ConnectionFailure(f" ==== MONGODB CONNECTION UNSUCCESSFUL ==== ")

    # Collect database information
    database = client[db_name]
    table_result = table_name in database.list_collection_names()
    num_of_docs_ = database[table_name].count_documents({})

    return {'mongo_status': 'Connected', 'table_name': table_name,
            'mongo_col': table_result, 'num_of_docs': num_of_docs_}


if __name__ == '__main__':

    args = parse_args()
    status = check_mongodb(args.db_name, args.table_name)
    check_pipeline_metadata("gsmls_airflow_pipeline", key=args.key, status=status)
    sys.exit(0)

