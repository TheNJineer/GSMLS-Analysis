import os
import re
import argparse
import pandas as pd
import pyspark.pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, upper
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType, FloatType
from gsmls.utility_func import create_postgres_connection, get_filepath
from gsmls.utility_func import check_pipeline_metadata, create_sql_engine
from gsmls.GSMLS_Cleaning import GSMLSCleaning
from py4j.protocol import Py4JJavaError


def check_duplicate_rows(mlsnums: list):

    engine = create_sql_engine("nj_tax_assessor")
    query = """SELECT mlsnum FROM gsmls_imputed_data"""
    df = pd.read_sql_query(query, con=engine)
    main_mlsnums = set(df['mlsnum'].tolist())
    target_mlsnums = set(mlsnums)

    new_mlsnums = target_mlsnums.difference(main_mlsnums)

    assert len(new_mlsnums) > 0, ' ==== ERROR: ALL TARGET DATA ARE DUPLICATES OF THE MAIN DATA. ENDING PROGRAM ==== '

    print(f' ==== {len(new_mlsnums)} ROWS OF NEW DATA WILL BE APPENDED TO gsmls_imputed_data ==== ')

    if (len(target_mlsnums) - len(new_mlsnums)) > 0:
        print(f' ==== {len(target_mlsnums) - len(new_mlsnums)} DUPLICATE ROWS FOUND IN THE DATA. NOW REMOVING ==== ')

    return new_mlsnums


def create_partial_address(df):

    print(' ==== CREATING PARTIAL ADDRESSES COLUMN ON GSMLS DATA ==== ')
    df = df.withColumn('PARTIAL_ADDRESS', substring(upper(col('ADDRESS')), 1, 9))

    return df


def create_spark_session():

    spark_obj = (SparkSession.builder
                 .appName("GSMLS_StageTwo_Cleaning")
                 .master("spark://spark:7077")
                 .config("spark.sql.warehouse.dir", "/workspace/data/stage_two")
                 .config("spark.sql.ansi.enabled", "false")
                 .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                 .config("spark.sql.sources.commitProtocolClass",
                         "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
                 .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                 .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
                 ).getOrCreate()

    return spark_obj


def create_tax_data(**kwargs):

    print(' ==== ACQUIRING TAX DATA FROM POSTGRESQL ==== ')
    tax_data = kwargs['spark'].read.jdbc(url=kwargs['jdbc_url'],
                                         table=kwargs['table_name'], properties=kwargs['properties'])
    tax_data = tax_data.withColumn('PARTIAL_ADDRESS', substring(upper(col('property_location')), 1, 9))
    tax_data = tax_data.withColumn('year_built', col('yearbuilt'))
    tax_data = tax_data.withColumn('lotsize_sqft', col('acreage') * 43560)
    tax_data = tax_data.select("municipality", "block", "lot", "property_location", "PARTIAL_ADDRESS", "building_sqft",
                               "lotsize_sqft", "year_built")

    print(' ==== TAX DATA ACQUIRED FROM POSTGRESQL ==== ')

    return tax_data


def data_cleaning(df):

    tax_id_pattern = re.compile(r'\d{4}-\d{5}-[0-9A-Z]*-\d{5}-[0-9A-Z]*-([0-9A-Z]*)?|0000-00000-0000-00000-0000')
    drop_cols = ["municipality", "block", "lot", "property_location", "PARTIAL_ADDRESS"]
    df['tax_pattern'] = df['TAXID'].str.count(tax_id_pattern)
    data_df = df.drop(columns=drop_cols)
    data_df = data_df.pipe(rename_columns, stage="stage_two")
    data_df = data_df.pipe(reorder_columns, stage="stage_two")

    return data_df


def data_metadata_cleanup(spark_df):

    print(f' ==== CLEANING THE DATAFRAME METADATA ==== ')
    # 1. Standardize types that trigger the pandas_api assertion
    for col_name, dtype in spark_df.dtypes:
        # Convert No-Time-Zone to standard Timestamp
        if dtype == 'timestamp_ntz':
            spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(TimestampType()))

        # Convert Short (int16) to Integer (int32)
        elif dtype == 'smallint':  # ShortType
            spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

    # 2. Complete Metadata Strip
    # We recreate the schema manually to ensure NO hidden Parquet metadata remains
    new_schema = spark_df.schema
    for field in new_schema.fields:
        field.metadata = {}

    # Apply the stripped schema
    spark_df = spark_df.sql_ctx.createDataFrame(spark_df.rdd, new_schema)

    # 3. Now convert
    print(f' ==== DATAFRAME METADATA HAS BEEN RESTORED ==== ')
    return spark_df


def data_stats(spark_df):

    # Spark.Dataframe needs to be cleaned of all metadata before pandas api used or error occurs (AssertionError)
    spark_df = data_metadata_cleanup(spark_df)

    data_df = spark_df.pandas_api()
    df_len = len(data_df)
    old_sqft_null_values = len(data_df[(data_df['SQFTAPPROX'].isna()) | (data_df['SQFTAPPROX'] == 0)])
    new_sqft_null_values = len(data_df[(data_df['building_sqft'].isna()) | (data_df['building_sqft'] == 0)])
    lotsize_null_values = len(data_df[(data_df['LOTSIZE (SQFT)'] == 0) | (data_df['LOTSIZE (SQFT)'].isna()) | (
                data_df['LOTSIZE (SQFT)'] < 0)])
    lotsize_new_values = len(
        data_df[(data_df['lotsize_sqft'] == 0) | (data_df['lotsize_sqft'].isna()) | (data_df['lotsize_sqft'] < 0)])

    print(f' ==== ORIGINAL SQFT NULL/ZERO VALUES (%): {old_sqft_null_values / df_len} ==== ')
    print(f' ==== NEW SQFT NULL/ZERO VALUES (%): {new_sqft_null_values / df_len} ==== ')
    print(f' ==== ORIGINAL LOTSIZE NULL/ZERO VALUES (%): {lotsize_null_values / df_len} ==== ')
    print(f' ==== NEW LOTSIZE NULL/ZERO VALUES (%): {lotsize_new_values / df_len} ==== ')

    return data_df


def data_quality_check(df, stage: str):

    data_dict = {}
    engine = create_sql_engine("nj_tax_assessor")
    target_columns = list(df.columns)

    if stage == 'stage_two':
        query = "SELECT * FROM gsmls_imputed_data LIMIT 1"

        temp_df = pd.read_sql_query(query, con=engine)
        check_columns = list(temp_df.columns)

    elif stage == 'stage_three':
        query = "SELECT * FROM cleaned_data_for_dnn LIMIT 1"

        temp_df = pd.read_sql_query(query, con=engine)
        check_columns = list(temp_df.columns)

    for col1, col2 in zip(target_columns, check_columns):
        if col1 == col2:
            data_dict[col1] = True
        else:
            data_dict[col1] = False

    difference = set(target_columns).difference(set(check_columns))
    results = [i for i in list(data_dict.keys()) if data_dict[i] is False]

    if len(results) > 0:
        print(f' ==== THE FOLLOWING COLUMNS ARE NOT ALIGNED WIT THE {stage.upper()} DATABASE ==== ')
        print(results)

        return {'alignment': False, 'same_columns': False}
    else:
        print(f' ==== ALL COLUMNS FOR {stage.upper()} ARE ALIGNED ==== ')

    if len(difference) > 0:
        print(f' ==== THE FOLLOWING COLUMNS EXIST IN THE DATA THAT DONT IN THE {stage.upper()} DATABASE ==== ')
        print(difference)

        return {'alignment': True, 'same_columns': False}
    else:
        print(f' ==== ALL COLUMNS FOR {stage.upper()} EXIST IN THE DATA ==== ')

        return {'alignment': True, 'same_columns': True}


def delete_parquet_file(abs_path: str):

    if os.path.isfile(abs_path):
        os.remove(abs_path)


def find_duplicate_mlsnums():

    mls_pattern = re.compile(r'Detail: Key \(mlsnum, status_short, town, county, closeddate\)='
                             r'\((\d{6,10}), .*, \d{4}-\d{2}-\d{2} 00:00:00\) already exists\.')
    pyspark_log_file = get_latest_log_file()
    mlsnum_list = []

    with open(pyspark_log_file, 'r') as file:
        for line in file:
            if mls_pattern.search(line) is not None:
                mlsnum_list.append(mls_pattern.search(line).group(1))

    return set(mlsnum_list)


def get_latest_log_file():

    pyspark_dir = get_filepath("pyspark_logs")
    files_list = os.listdir(pyspark_dir)
    dir_list = [directory for directory in files_list if os.path.isdir(os.path.join(pyspark_dir, directory))]
    latest_app_id = dir_list[-1]

    return os.path.join(pyspark_dir, latest_app_id, '0', 'stderr')


def join_dfs(gsmls_df, tax_df):

    print(' ==== MERGING THE GSMLS & TAX DATA ==== ')
    joined_df = gsmls_df.join(tax_df, (gsmls_df["NJ_TOWNCODE"] == tax_df["municipality"])
                            & (gsmls_df["PARTIAL_ADDRESS"] == tax_df["PARTIAL_ADDRESS"]), "left")
    joined_df = joined_df.drop(gsmls_df["PARTIAL_ADDRESS"])
    joined_df = joined_df.dropDuplicates(subset=["MLSNUM"])

    print(' ==== THE GSMLS & TAX DATA MERGE IS COMPLETE ==== ')

    return joined_df


def parse_args():

    parser = argparse.ArgumentParser(description='stage one cleaning')
    parser.add_argument("--table_name", required=True)

    # return parser.parse_args(['--table_name', 'res_properties'])
    return parser.parse_args()


def pyspark_save_data(spark_df, **kwargs):

    duplicate_pattern = re.compile(r'ERROR: duplicate key value violates unique constraint "gsmls_imputed_data_key"')

    try:
        spark_df.write.jdbc(kwargs['jdbc_url'], 'gsmls_imputed_data',
                            mode="append", properties=kwargs['properties'])
    except Py4JJavaError as pje:
        if duplicate_pattern.search(str(pje)) is not None:
            print(' ==== DUPLICATE DATA DISCOVERED IN TARGET DATA ==== ')
            mlsnum_list = find_duplicate_mlsnums()
            new_df = remove_duplicate_row(spark_df, mlsnum_list)
            pyspark_save_data(new_df, **kwargs)
        else:
            raise Py4JJavaError


def remove_duplicate_row(spark_df, mlsnum: list | set, flag=None):

    if flag is None:
        print(f' ==== REMOVING DUPLICATE ROW WITH MLSNUM(s): {mlsnum} ==== ')
        return spark_df.where(~col("mlsnum").isin(list(mlsnum)))
    else:
        return spark_df.where(col("mlsnum").isin(list(mlsnum)))


def reorder_columns(target_data: pd.DataFrame, stage: str):

    engine = create_sql_engine("nj_tax_assessor")

    if stage == 'stage_two':
        query = "SELECT * FROM gsmls_imputed_data LIMIT 1"

        temp_df = pd.read_sql_query(query, con=engine)
        target_columns = list(temp_df.columns)

        return target_data[target_columns]

    elif stage == 'stage_three':
        query = "SELECT * FROM cleaned_data_for_dnn LIMIT 1"

        temp_df = pd.read_sql_query(query, con=engine)
        target_columns = list(temp_df.columns)

        return target_data[target_columns]


def rename_columns(df: pyspark.pandas.DataFrame, stage: str):

    if stage == 'stage_two':
        df.columns = df.columns.str.lower()
        df.rename(columns={'1_unit_ac': 'one_unit_ac', 'olp/lp%': 'olp_lp', 'sp/lp%': 'sp_lp',
                           '1_car_wide': 'one_car_wide', '2_car_wide': 'two_car_wide',
                           '3_units_ac': 'three_units_ac', '2_units_ac': 'two_units_ac',
                           'lotsize (sqft)': 'lotsize_sqft_orig'}, inplace=True)

        return df

    elif stage == 'stage_three':
        df.rename(columns={'one_unit_ac': '1_unit_ac', 'olp_lp': 'olp/lp%', 'sp_lp': 'sp/lp%',
                           'one_car_wide': '1_car_wide', 'two_car_wide': '2_car_wide',
                           'three_units_ac': '3_units_ac', 'two_units_ac': '2_units_ac',
                           'lotsize_sqft_orig': 'lotsize (sqft)'}, inplace=True)
        df.columns = df.columns.str.upper()
        df.rename(columns={'LOTSIZE_SQFT': 'lotsize_sqft', 'YEAR_BUILT': 'year_built',
                           'BUILDING_SQFT': 'building_sqft'}, inplace=True)

        return df


def stage_one(path: str):
    """

    """
    arg = parse_args()
    obj = GSMLSCleaning(table_name=arg.table_name)

    starting_data = obj.initial_data_loading('stage_one')

    if len(starting_data) > 0:
        # Cast 'EXPIREDATE' col to pd.datetime64[ns]. Will be used to fillna in Stage Two
        starting_data['EXPIREDATE'] = pd.to_datetime(starting_data['EXPIREDATE'])
        starting_data['CLOSEDDATE'] = starting_data['CLOSEDDATE'].fillna(starting_data['EXPIREDATE'])
        refined_data = starting_data.pipe(GSMLSCleaning.remove_columns)
        min_date = refined_data['LISTDATE'].min()
        mortgage_data = GSMLSCleaning.get_mortgage_rates(min_date)

        refined_data = (refined_data.pipe(GSMLSCleaning.impute_mortgage_rates, mortgage_df=mortgage_data)
                        .pipe(obj.create_nj_county_code)
                        .pipe(GSMLSCleaning.create_nj_town_code)
                        .pipe(GSMLSCleaning.fix_string_data)
                        .pipe(GSMLSCleaning.pre_cleaning)
                        .pipe(GSMLSCleaning.expired_reclassification)
                        .pipe(GSMLSCleaning.object_to_str))
        # Pull the mlsids of the current data to update PYSPARK_PROCESSED values to true
        mls_list = refined_data['MLSNUM'].tolist()
        final_data = refined_data.pipe(GSMLSCleaning.filter_data)

        # File will be used by PySpark. Pandas saves timestamps in nanoseconds which cant be read by Spark
        # Need to coerce/convert to microseconds(us) before saving
        # /workspace directory is debug environment while /app is production env. Make sure I distinguish
        final_data.to_parquet(os.path.join(path, "refined_data.parquet"), index=False,
                              engine='pyarrow', coerce_timestamps='us', allow_truncated_timestamps=True)
        print(f' ==== DATA SAVED TO PARQUET FILE ==== ')

        return obj, mls_list
    else:
        print(f' ==== NO NEW DATA. ENDING STAGE ONE CLEANING ==== ')
        return None, None


def stage_two(path: str, mlsnum_list: list, **kwargs):
    """

    """

    table_name = "nj_tax_assessor_data"
    jdbc_url, properties = create_postgres_connection('jdbc', 'nj_tax_assessor')
    print(' ==== CREATING PYSPARK SPARKSESSION OBJECT ==== ')
    spark = create_spark_session()
    kwargs = {'jdbc_url': jdbc_url,
              'properties': properties,
              'table_name': table_name,
              'spark': spark}

    # Check directory before final production
    df = spark.read.parquet(os.path.join(path, "refined_data.parquet"))
    df = create_partial_address(df)
    tax_df = create_tax_data(**kwargs)
    final_df = join_dfs(df, tax_df)
    final_df = data_stats(final_df)  # Prints imputed stats and returns a pandas-on-Spark dataframe
    final_df = data_cleaning(final_df)
    quality_results = final_df.pipe(data_quality_check, stage='stage_two')
    target_mlsnums = check_duplicate_rows(mlsnum_list)
    final_df = final_df.to_spark()
    print(f' ==== Dataframe length before duplicate check: {final_df.count()} ==== ')
    final_df = remove_duplicate_row(final_df, target_mlsnums, flag='pre-save')
    print(f' ==== Dataframe length after duplicate check: {final_df.count()} ==== ')

    if final_df.count() > 0:
        if quality_results['alignment'] is False and quality_results['same_columns'] is False:
            print(f' ==== DATA DIDNT SAVE TO GSMLS_IMPUTED DATA. DATA COLUMNS ARE NOT ALIGNED WITH DATABASE ==== ')
        elif quality_results['same_columns'] is False:
            print(" ==== DATA DID NOT SAVE. UNVERIFIED COLUMNS EXISTING IN DATA ==== ")
        else:
            pyspark_save_data(final_df, **kwargs)
            print(f" ==== NEW DATA APPENDED TO gsmls_imputed_data ====/n==== TOTAL NEW ROWS APPENDED: {len(final_df.count())}  ====")
    else:
        print(" ==== THERE IS NO NEW DATA TO ADD TO gsmls_imputed_data ==== ")


def stage_three(obj: GSMLSCleaning, mlsnum_list: list, path: str):
    """

    """

    starting_data = obj.initial_data_loading('stage_three')
    starting_data = starting_data.pipe(rename_columns, stage='stage_three')
    starting_data = starting_data.pipe(GSMLSCleaning.remove_columns, phase='stage_three_a')
    refined_data = (starting_data.pipe(GSMLSCleaning.convert_bool_columns)
                    .pipe(GSMLSCleaning.fix_lotsize)
                    .pipe(GSMLSCleaning.fix_property_valuation))
    refined_data.loc[:, 'assess_to_lp_ratio'] = refined_data['ASSESSTOTAL'] / refined_data['LISTPRICE']
    agg_data = GSMLSCleaning.create_aggregate_data(refined_data)
    refined_data = refined_data.pipe(GSMLSCleaning.impute_null_data, agg_data=agg_data)
    agg_data = agg_data.pipe(GSMLSCleaning.recalculate_agg_data, test_data=refined_data)
    refined_data = refined_data.pipe(GSMLSCleaning.create_zscore_cols, agg_data=agg_data)
    refined_data = refined_data.pipe(GSMLSCleaning.remove_columns, phase='stage_three_b')
    refined_data['property_valuation'] = refined_data['assess_to_lp_zscore'].apply(GSMLSCleaning.property_valuation)
    refined_data = refined_data.pipe(reorder_columns, "stage_three")
    refined_data = refined_data.pipe(GSMLSCleaning.filter_data, phase='stage_three')

    # Save the refined data to PostgreSQL
    refined_data.pipe(data_quality_check, stage='stage_three')
    obj.save_cleaned_data(refined_data)
    # Mark the properties in the MLS List as PYSPARK PROCESSED and delete parquet file from Stage One
    GSMLSCleaning.update_pyspark_processed(mlsnum_list, 'RES')
    delete_parquet_file(os.path.join(path, "refined_data.parquet"))
    print(' ==== STAGE THREE CLEANING COMPLETE ==== ')


if __name__ == '__main__':

    print(' ==== STAGE ONE CLEANING INITIATED ==== ')
    filepath = get_filepath('refined_data')
    check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None, key_="cleaning_completed", status_=False)
    clean_obj, target_mls_list = stage_one(filepath)

    if clean_obj is not None and target_mls_list is not None:
        print(' ==== STAGE TWO CLEANING INITIATED ====  ')
        stage_two(filepath, target_mls_list)
        print(' ==== STAGE THREE CLEANING INITIATED ==== ')
        stage_three(clean_obj, target_mls_list, path=filepath)
        print(' ==== ALL STAGES OF DATA TRANSFORMATION HAS BEEN COMPLETED ==== ')
        check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None,
                                key_="cleaning_completed", status_=True)
    else:
        print(' ==== NO NEW DATA TO TRANSFORM. JOB HAS BEEN COMPLETED ==== ')
        check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None,
                                key_="cleaning_completed", status_=True)


