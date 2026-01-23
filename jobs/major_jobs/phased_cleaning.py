import os
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, upper
from gsmls_core.gsmls.utility_func import create_postgres_connection, get_filepath
from gsmls_core.gsmls.utility_func import check_pipeline_metadata, create_sql_engine
from gsmls_core.gsmls.GSMLS_Cleaning import GSMLSCleaning


def cleaning_stats(spark_df):

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

    data_df = data_df.drop(columns=["municipality", "block", "lot", "property_location", "PARTIAL_ADDRESS"])

    return data_df.to_spark()


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

    return parser.parse_args(['--table_name', 'res_properties'])


def reorder_columns(target_data: pd.DataFrame):

    engine = create_sql_engine("nj_tax_assessor")
    query = "SELECT * FROM cleaned_data_for_dnn LIMIT 1"

    temp_df = pd.read_sql_query(query, con=engine)
    target_columns = list(temp_df.columns)

    return target_data[target_columns]


def stage_one(path: str):
    arg = parse_args()
    obj = GSMLSCleaning(table_name=arg.table_name)

    starting_data = obj.initial_data_loading('stage_one')
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

    # File will be used by PySpark. Pandas saves timestamps in ns which cant be read by Spark
    # Need to coerce/convert to nanoseconds(us) before saving
    # /workspace directory is debug environment while /app is production env. Make sure I distinguish
    final_data.to_parquet(os.path.join(path, "refined_data.parquet"), index=False,
                          engine='pyarrow', coerce_timestamps='us', allow_truncated_timestamps=True)
    print(f' ==== DATA SAVED TO PARQUET FILE ==== ')

    return obj, mls_list


def stage_two(path: str):

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
    final_df = cleaning_stats(final_df)  # Prints imputed stats and returns a pandas-on-Spark dataframe
    print(' ==== SAVING STAGE TWO DATA TO POSTGRESQL ==== ')
    print(' ==== OVERWRITING JOB_GSMLS_IMPUTED_DATA ==== ')
    # final_df.to_parquet("/workspace/data/stage_two/gsmls_imputed_data.parquet", index=False, mode='overwrite')
    final_df.write.jdbc(jdbc_url, 'jobs_gsmls_imputed_data', mode="overwrite", properties=properties)


def stage_three(obj: GSMLSCleaning, mls_list: list, path=None):

    starting_data = obj.initial_data_loading('stage_three')
    starting_data = starting_data.pipe(GSMLSCleaning.remove_columns, phase='stage_two')
    refined_data = (starting_data.pipe(GSMLSCleaning.convert_bool_columns)
                    .pipe(GSMLSCleaning.fix_lotsize)
                    .pipe(GSMLSCleaning.fix_property_valuation))
    refined_data.loc[:, 'assess_to_lp_ratio'] = refined_data['ASSESSTOTAL'] / refined_data['LISTPRICE']
    agg_data = GSMLSCleaning.create_aggregate_data(refined_data)
    refined_data = refined_data.pipe(GSMLSCleaning.impute_null_data, agg_data=agg_data)
    agg_data = agg_data.pipe(GSMLSCleaning.recalculate_agg_data, test_data=refined_data)
    refined_data = refined_data.pipe(GSMLSCleaning.create_zscore_cols, agg_data=agg_data)
    refined_data = refined_data.pipe(GSMLSCleaning.remove_columns, phase='stage_three')
    refined_data['property_valuation'] = refined_data['assess_to_lp_zscore'].apply(GSMLSCleaning.property_valuation)
    refined_data = refined_data.pipe(reorder_columns)
    refined_data = refined_data.pipe(GSMLSCleaning.filter_data, phase='stage_three')

    # Save the refined data to PostgreSQL
    print(f' ==== SIZE OF MLS lIST: {len(mls_list)}')
    obj.save_cleaned_data(refined_data)
    print(f' ==== NEW DATA HAS BEEN APPENDED TO cleaned_data_for_dnn ==== ')
    # Mark the properties in the MLS List as PYSPARK PROCESSED
    # GSMLSCleaning.update_pyspark_processed(mls_list, arg.table_name)
    print(' ==== STAGE THREE CLEANING COMPLETE ==== ')


if __name__ == '__main__':

    print(' ==== STAGE ONE CLEANING INITIATED ==== ')
    filepath = get_filepath('refined_data')
    check_pipeline_metadata("gsmls_cleaning_pipeline", key="cleaning_completed", status=False)
    clean_obj, target_mls_list = stage_one(filepath)
    print(' ==== STAGE ONE CLEANING COMPLETE ==== ')
    print(' ==== STAGE TWO CLEANING INITIATED ==== ')
    stage_two(filepath)
    print(' ==== STAGE TWO CLEANING COMPLETE ==== ')
    print(' ==== STAGE THREE CLEANING INITIATED ==== ')
    stage_three(clean_obj, target_mls_list, path=filepath)
    print(' ==== ALL STAGES OF DATA TRANSFORMATION HAS BEEN COMPLETED ==== ')
    check_pipeline_metadata("gsmls_cleaning_pipeline", key="cleaning_completed", status=True)

