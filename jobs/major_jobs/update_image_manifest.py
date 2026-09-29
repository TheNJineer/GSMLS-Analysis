import argparse
import boto3
import json
import os
import pandas as pd
import pendulum
import psycopg2
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from math import floor
from gsmls.utility_func import get_filepath, create_sql_engine, create_postgres_connection
from random import shuffle
from sqlalchemy.exc import ProgrammingError


def assign_split_labels(df: pd.DataFrame):

    """
    Split the data up into 70% training, 15% validation, 15% testing
    """

    mlsnums = df['mlsnum'].unique().tolist()
    length = len(mlsnums)
    training = floor(length * 0.7) + 1
    val_test = floor(length * 0.15) + 1
    shuffle(mlsnums)  # randomizes the order of the mlsnums to be aassigned labels
    training_mlsnums = mlsnums[:training]
    val_mlsnums = mlsnums[training:training + val_test]
    test_mlsnums = mlsnums[training + val_test:]

    df.loc[df[(df['mlsnum'].isin(training_mlsnums)) & (df['room_label'] != 'other')].index, 'split_type'] = 'train'
    df.loc[df[(df['mlsnum'].isin(val_mlsnums)) & (df['room_label'] != 'other')].index, 'split_type'] = 'val'
    df.loc[df[(df['mlsnum'].isin(test_mlsnums)) & (df['room_label'] != 'other')].index, 'split_type'] = 'test'

    return df


def create_aws_ground_truth_jsonl(df: pd.DataFrame, label: str | list, client):
    df = df[['room_label', 's3_uri', 'original_image']]

    if isinstance(label, str):
        filename = f'{label}_ground_truth.jsonl'
        temp_df = df[(df['room_label'] == label) & (df['original_image'] == True)]
        create_jsonl_file(temp_df, label)
        response = client.upload_file(filename, "amzn-s3-gsmls-propertyimages", filename)

    elif isinstance(label, list):
        for label_type in label:
            filename = f'{label_type}_ground_truth.jsonl'
            temp_df = df[(df['room_label'] == label_type) & (df['original_image'] == True)]
            create_jsonl_file(temp_df, label_type)
            response = client.upload_file(filename, "amzn-s3-gsmls-propertyimages", filename)


def create_jsonl_file(df: pd.DataFrame, label: str):

    df = df[:5000]

    with open(f'{label}_ground_truth.jsonl', 'w') as writer:
        for _, data in df.iterrows():
            writer.write(f"{json.dumps({'source-ref': data['s3_uri']})}\n")


def create_s3_client():

    load_aws_env()
    return boto3.client('s3')


def create_manifest_table():

    _, properties = create_postgres_connection('psycopg2', 'gsmls')


    query = """
        CREATE TABLE IF NOT EXISTS gsmls_image_manifest(
            image_id text,
            source varchar(10),
            room_label varchar(25),
            room_quality varchar(15),
            size_kb real,
            label_conf real,
            quality_conf real,
            remarks_sentiment real,
            split_type varchar(5),
            flags varchar(50),
            original_image boolean,
            last_modified timestamp without time zone,
            mlsnum integer,
            s3_uri text,
            manifest_date date
        );
            
    """

    with psycopg2.connect(**properties) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)


def filter_target_df(df: pd.DataFrame, existing_data: list):

    return df[~df['mlsnums'].isin(existing_data)]  # Remove the rows which exist in the existing data


def load_aws_env():

    filepath = get_filepath("env")
    load_dotenv(filepath)


def load_existing_data(sql_engine):
    """
    Retrieve only the mlsnums if the table exists. If not, return False
    """

    query = """
        SELECT mlsnum FROM gsmls_image_manifest
    """

    try:
        df = pd.read_sql_query(query, con=sql_engine)

        if df.empty:
            return False
        else:
            return df['mlsnum'].unique().tolist()

    except ProgrammingError:
        return False


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Image Manifest Cleaning')
    parser.add_argument("--date_str", required=True)
    parser.add_argument("--save_type", required=True)

    return parser.parse_args(['--date_str', '2026-05-03T01-00Z', '--save_type', 'ground_truth'])
    # return parser.parse_args()


def parse_raw_data(df: pd.DataFrame):

    """
    Step 1: Remove all rows where df['Size'] == nan
    Step 2: Remove all rows where df['Bucket'] doesn't start with 'raw/'
    Step 3: Reset the index to numerical instead of 'amzn-s3-...'
    Step 4: Create a column for mlsnum by parsing the 'Bucket' column
    Step 5: Create a column for image_id by parsing the 'Bucket' column
    Step 6: Create a column for s3_uri by parsing the 'Bucket' column and combining with the index column. ***Put the
    correct s3_uri in the column. Crucial for input pipeline to tensorflow
    Step 7: Create a column for room_label by parsing the 'Bucket' column
    Step 8: Create a column for room_quality by parsing the 'Bucket' column
    Step 9: Create a column for source by parsing the 'Index' column
    Step 10: Create a column for listing_sentiment with a default value of 0.0
    Step 11: Create a column for label confidence with a default value of 0.0
    Step 12: Create a column for quality confidence with a default value of 0.0
    Step 13: Create a column for split_type with a default value of nan
    Step 14: Create a column for flags with a default value of nan
    Step 15: Create a column for last_updated with a default value of today's date
    Step 16: Rename VersionId column to 'duplicate' for duplicate images
    Step 17: Rename Size column to 'size_mb' and recalculate the values to be for MB
    """

    columns_list = ['image_id', 'source', 'room_label',
                    'room_quality', 'size_kb', 'label_conf', 'quality_conf',
                    'remarks_sentiment', 'split_type',
                    'flags', 'original_image', 'last_modified',
                    'mlsnum', 's3_uri', 'manifest_date']
    alphabets = [i.upper() for i in 'abcdefghijklmnopqrstuvwxyz']

    print(' ==== PARSING RAW MANIFEST DATA ==== ')

    print(' ==== REMOVING ROWS WITH ILL-FORMED IMAGE PATHS ==== ')
    temp_df = df.reset_index()
    valid_path_mask = temp_df[temp_df['Bucket'].str.startswith('raw/')].index
    temp_df = temp_df.loc[valid_path_mask]
    print(' ==== REMOVING ROWS WITH IMAGE SIZE EQUAL TO NULL ==== ')
    temp_df = temp_df[~temp_df['Size'].isnull()]
    print(' ==== REFORMATTING IMAGE PATHS ==== ')
    temp_df['Bucket'] = temp_df['Bucket'].str.replace('%20', ' ')
    print(' ==== EXTRACTING MLSNUMS ==== ')
    temp_df['mlsnum'] = temp_df['Bucket'].str.extract(r"(\d{7,8})")
    image_name = temp_df['Bucket'].str.split('-').str.get(2).str.replace('.png', '')
    print(' ==== CREATING UNIQUE IMAGE IDS ==== ')
    temp_df['image_id'] = temp_df['mlsnum'].str.cat(image_name, sep='-')
    temp_df['index'] = temp_df['index'].str.replace('amzn', 's3://amzn')
    print(' ==== CREATING CORRECT S3 URI PATHS ==== ')
    temp_df['s3_uri'] = temp_df['index'].str.cat(temp_df['Bucket'], sep='/')
    parsed_bucket = temp_df['Bucket'].str.split('/')
    print(' ==== CREATING COLUMNS FOR ROOM LABELS AND ROOM QUALITY ==== ')
    temp_df['room_label'] = parsed_bucket.str.get(3)
    valid_room_mask = temp_df[temp_df['room_label'].str.startswith(tuple(alphabets))].index
    temp_df = temp_df.loc[valid_room_mask]
    parsed_bucket = temp_df['Bucket'].str.split('/')
    temp_df['room_quality'] = parsed_bucket.str.get(4).str.lower()
    temp_df['source'] = temp_df['index'].str.split('-').str.get(2)
    temp_df['room_label'] = temp_df['room_label'].str.lower()
    temp_df['label_conf'] = 0.0
    temp_df['quality_conf'] = 0.0
    temp_df['remarks_sentiment'] = 0.0
    temp_df['split_type'] = None
    temp_df['flags'] = None
    temp_df['manifest_date'] = pendulum.now().date()
    print(' ==== RECALCULATING IMAGES SIZE DATA ==== ')
    temp_df['Size'] = temp_df['Size'] / 1000
    print(' ==== DROPPING UNWANTED COLUMNS ==== ')
    temp_df.rename(columns={'VersionId': 'original_image', 'Size': 'size_kb', 'LastModifiedDate': 'last_modified'}, inplace=True)
    temp_df.drop(columns=['index', 'Bucket', 'Key', 'IsLatest'], inplace=True)
    final_df = temp_df[columns_list]
    final_df = final_df[(~final_df['mlsnum'].isna()) & (final_df['room_quality'].isin(['fixer upper', 'unknown']))]
    print(' ==== FINAL DATAFRAME COMPLETE ==== ')
    # Filter to train for only bathroom, bedroom, backyard, basement, kitchen, living room, foyer/entrance, closet,
    # deck, dining room, fireplace, gym, laundry, office, pool, sun room, tax map, floor plans
    return final_df


def retrieve_latest_manifest(date_str: str, client):

    """
    date_str: format YYYY-MM-DDTNN-00Z
    """

    bucket = 'amzn-s3-gsmls-propertyimages'
    # Path to the manifest.json for a specific date
    manifest_key = f'{bucket}/gsmls_database_manifests/{date_str}/manifest.json'

    try:
        # Download the manifest file
        response = client.get_object(Bucket=bucket, Key=manifest_key)
        # Loads JSON string or bytestring
        manifest = json.loads(response['Body'].read().decode('utf-8'))

        print(' ==== GSMLS IMAGE MANIFEST ACQUIRED ==== ')
        return manifest

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(' ==== GSMLS IMAGE MANIFEST NOT ACQUIRED: KEY NOT FOUND ==== ')
            return None


def retrieve_compressed_file(manifest, client):

    """
    manifest: dict object
    """

    bucket = 'amzn-s3-gsmls-propertyimages'

    for file in manifest['files']:

        # compressed csv file
        compressed_file = file['key']

        # 3. Download the data file (usually .csv.gz)
        data_obj = client.get_object(Bucket=bucket, Key=compressed_file)

        # 4. Read directly into Pandas
        # S3 Inventory CSVs don't have headers, so you define them based on your inventory config
        df = pd.read_csv(data_obj['Body'], compression='gzip', names=[
            'Bucket', 'Key', 'VersionId', 'IsLatest', 'Size', 'LastModifiedDate'])

        return df


def save_data(df: pd.DataFrame, existing_data, sql_engine):

    if isinstance(existing_data, bool):
        # Bool value of 'False' would come from load_existing_data()
        create_manifest_table()
        df = assign_split_labels(df)
        df.to_sql("gsmls_image_manifest", con=sql_engine, if_exists="append", index=False)

    else:
        filtered_df = filter_target_df(df, existing_data)

        if not filtered_df.empty:
            filtered_df = assign_split_labels(filtered_df)
            filtered_df.to_sql("gsmls_image_manifest", con=sql_engine, if_exists="append", index=False)


if __name__ == '__main__':

    arg = parse_args()
    s3_client = create_s3_client()
    engine = create_sql_engine('gsmls', remote=True)
    current_mlsnums = load_existing_data(engine)
    manifest_obj = retrieve_latest_manifest(arg.date_str, s3_client)

    if manifest_obj is not None:
        raw_df = retrieve_compressed_file(manifest_obj, s3_client)
        main_df = parse_raw_data(raw_df)

        if arg.save_type == 'ground_truth':
            create_aws_ground_truth_jsonl(main_df, 'bathroom', s3_client)
        elif arg.save_type == 'manifest':
            save_data(main_df, current_mlsnums, engine)
