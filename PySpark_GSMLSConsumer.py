from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def write_to_postgres(batch_df):
    jdbc_url = "jdbc:postgresql://<hostname>:<port>/<database>"
    jdbc_properties = {
        "user": "<username>",
        "password": "<password>",
        "driver": "org.postgresql.Driver"
    }

    batch_df.write \
        .jdbc(url=jdbc_url, table="your_table_name", mode="append", properties=jdbc_properties)

# Define the schema for the JSON data
# 3rd argument of the StructField object allows the field to be nullable
schema = StructType([
	StructField("MLSNUM", StringType(), True),
	StructField("STATUS_SHORT", StringType(), True),
	StructField("STREETNUMDISPLAY", StringType(), True),
	StructField("STREETNAME", StringType(), True),
	StructField("TOWN",StringType(), True),
	StructField("COUNTY", StringType(), True),
	StructField("ZIPCODE", StringType(), True),
	StructField("TOWNCODE", IntegerType(), True),
	StructField("COUNTYCODE", IntegerType(), True),
	StructField("BLOCKID", StringType(), True),
	StructField("LOTID", StringType(), True),
	StructField("TAXID", StringType(), True),
	StructField("DAYSONMARKET", IntegerType(), True),
	StructField("ORIGLISTPRICE", IntegerType(), True),
	StructField("LISTPRICE", IntegerType(), True),
	StructField("SALESPRICE", IntegerType(), True),
	StructField("SP/LP%", StringType(), True),
	StructField("LOANTERMS_SHORT", StringType(), True),
	StructField("ROOMS", IntegerType(), True),
	StructField("BEDS", IntegerType(), True),
	StructField("BATHSTOTAL", DoubleType(), True),
	StructField("SQFTAPPROX", IntegerType(), True),
	StructField("ACRES", DoubleType(), True),
	StructField("LOTSIZE", StringType(), True),
	StructField("ASSESSAMOUNTBLDG", IntegerType(), True),
	StructField("ASSESSAMOUNTLAND", IntegerType(), True),
	StructField("ASSESSTOTAL", IntegerType(), True),
	StructField("SUBPROPTYPE", StringType(), True),
	StructField("STYLEPRIMARY_SHORT", StringType(), True),
	StructField("STYLE_SHORT", StringType(), True),
	StructField("SUBDIVISION", StringType(), True),
	StructField("TAXAMOUNT", DoubleType(), True),
	StructField("TAXRATE", DoubleType(), True),
	StructField("TAXYEAR", IntegerType(), True),
	StructField("YEARBUILT", IntegerType(), True),
	StructField("LISTDATE", StringType(), True),
	StructField("PENDINGDATE", StringType(), True),
	StructField("ANTICCLOSEDDATE", StringType(), True),
	StructField("CLOSEDDATE", StringType(), True),
	StructField("EXPIREDATE", StringType(), True),
	StructField("WITHDRAWNDATE", StringType(), True),
	StructField("OWNERSHIP_SHORT", StringType(), True),
	StructField("EASEMENT_SHORT", StringType(), True),
	StructField("PARKNBRAVAIL", IntegerType(), True),
	StructField("DRIVEWAYDESC_SHORT", StringType(), True),
	StructField("GARAGECAP", IntegerType(), True),
	StructField("HEATSRC_SHORT", StringType(), True),
	StructField("HEATSYSTEM_SHORT", StringType(), True),
	StructField("COOLSYSTEM_SHORT", StringType(), True),
	StructField("WATER_SHORT", StringType(), True),
	StructField("UTILITIES_SHORT", StringType(), True),
	StructField("EXTERIOR_SHORT", StringType(), True),
	StructField("FIREPLACES", IntegerType(), True),
	StructField("FLOORS_SHORT", IntegerType(), True),
	StructField("POOL_SHORT", StringType(), True),
	StructField("ROOF_SHORT", StringType(), True),
	StructField("SEWER_SHORT", StringType(), True),
	StructField("SIDING_SHORT", StringType(), True),
	StructField("BASEMENT_SHORT", StringType(), True),
	StructField("BASEDESC_SHORT", StringType(), True),
	StructField("FLOODZONE", StringType(), True),
	StructField("ZONING", StringType(), True),
	StructField("APPFEE", IntegerType(), True),
	StructField("ASSOCFEE", IntegerType(), True),
	StructField("COMPBUY", StringType(), True),
	StructField("COMPSELL", StringType(), True),
	StructField("COMPTRANS", StringType(), True),
	StructField("LISTTYPE_SHORT", StringType(), True),
	StructField("OFFICELIST", StringType(), True),
	StructField("OFFICESELL", StringType(), True),
	StructField("OFFICESELLNAME", StringType(), True),
	StructField("AGENTSELLNAME", StringType(), True),
	StructField("OWNERNAME", StringType(), True),
	StructField("AGENTLIST", StringType(), True),
	StructField("AGENTSELL", StringType(), True),
	StructField("REMARKSAGENT", StringType(), True),
	StructField("REMARKSPUBLIC", StringType(), True),
	StructField("SHOWSPECIAL", StringType(), True),
	StructField("BUSRELATION", StringType(), True),
	StructField("LATITUDE", StringType(), True),
	StructField("LONGITUDE", StringType(), True),
	StructField("IMAGES", StringType(), True),
	StructField("MLS", StringType(), True),
	StructField("QTR", StringType(), True),
	StructField("CONDITION", StringType(), True)
])

spark = SparkSession.builder \
	.appName("GSMLSConsumer") \
	.master("spark://localhost:7077") \
	.config("spark.hadoop.home.dir", "/opt/hadoop") \
	.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3') \
	.getOrCreate()

pydf = spark \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "9094") \
	.option("subscribe", "res_properties") \
	.option("startingOffsets", "earliest") \
	.option("maxOffsetsPerTrigger", 100) \
	.load()

# 1) Fetches the data from Kafka as a column
# 2) Creates a StructType column called "json" from the "value" column. It parses the strings according to the schema provided
# 3) Select all the fields in the "json" column and flatten them into separate columns. The .select function selects only these fields
#    leaving the "value" and "json" columns behind

parsed_df = pydf.selectExpr("CAST(value AS STRING)") \
	.withColumn("json", from_json(col("value"), schema)) \
	.select(col("json*"))

"""
---------------------------------------------------------------
***APPLY TRANSFORMATIONS HERE***
---------------------------------------------------------------
"""

# Strip the values of unnecessary endings
stripped_df = parsed_df.withColumn('ACRES', F.rtrim('ACRES', '*')) \
	.withColumn('BLOCKID', F.rtrim('BLOCKID', '*')) \
	.withColumn('COUNTY', F.rtrim('COUNTY', '*')) \
	.withColumn('COUNTYCODE', F.rtrim('COUNTYCODE', '*')) \
	.withColumn('LOTID', F.rtrim('LOTID', '*')) \
	.withColumn('LOTSIZE', F.rtrim('LOTSIZE', '*')) \
	.withColumn('LOTID', F.rtrim('LOTID', '*')) \
	.withColumn('OWNERNAME', F.rtrim('OWNERNAME', '*')) \
	.withColumn('INVESTMENT_SALE', F.when(F.regexp('OWNERNAME', r'(?i)llc'), 1).otherwise(0)) \
	.withColumn('STREETNAME', F.rtrim('STREETNAME', '*')) \
	.withColumn('TAXID', F.rtrim('TAXID', '*')) \
	.withColumn('TOWN', F.regexp_replace('TOWN', r'\*\(\d{4}\*\)', '')) \
	.withColumn('TOWNCODE', F.rtrim('TOWNCODE', '*')) \
	.withColumn('ZIPCODE', F.rtrim('ZIPCODE', '*')) \
	.withColumn('SP/LP%', F.rtrim('SP/LP%', '%'))

# transform date columns from strings to datetime formats to do more transformations
transformed_df = stripped_df.withColumn('ANTICCLOSEDDATE', F.to_date('ANTICCLOSEDDATE')) \
	.withColumn('CLOSEDDATE', F.to_date('CLOSEDDATE')) \
	.withColumn('EXPIREDATE', F.to_date('EXPIREDATE')) \
	.withColumn('LISTDATE', F.to_date('LISTDATE')) \
	.withColumn('PENDINGDATE', F.to_date('PENDINGDATE')) \
	.withColumn('WITHDRAWNDATE', F.to_date('WITHDRAWNDATE')) \
	.withColumn('DAYS_TO_CLOSE', F.datediff('CLOSEDDATE', 'PENDINGDATE')) \
	.withColumn('ANTIC_CLOSEDATE_DIFF', F.datediff('CLOSEDDATE', 'ANTICCLOSEDDATE')) \
	.withColumn('LISTING_REMARKS', F.concat_ws('. ', stripped_df['REMARKSPUBLIC'],
											   stripped_df['REMARKSAGENT'], stripped_df['SHOWSPECIAL'])) \
	.withColumn('SP/LP%', (stripped_df['SP/LP%'].cast('int') - F.lit(100)) / F.lit(100)) \
	.withColumn('LOTSIZE_SQFT', stripped_df['ACRES'].cast('double') * F.lit(43560)) \
	.withColumn('ASSESSAMOUNTBLDG', F.regexp_replace('ASSESSAMOUNTBLDG', r'00:00:00', '0'))

# Create new columns for each interior/exterior finish or attribute
df = transformed_df.withColumn('ON_STREET_PKNG', F.regexp('DRIVEWAYDESC_SHORT', r'OnStreet')) \
	.withColumn('OFF_STREET_PKNG', F.regexp('DRIVEWAYDESC_SHORT', r'OffStret')) \
	.withColumn('1_CAR_WIDE', F.regexp('DRIVEWAYDESC_SHORT', r'1CarWide')) \
	.withColumn('2_CAR_WIDE', F.regexp('DRIVEWAYDESC_SHORT', r'2CarWide')) \
	.withColumn('WINDOW_AC', F.regexp('COOLSYSTEM_SHORT', r'WindowAC')) \
	.withColumn('CENTRAL_AC', F.regexp('COOLSYSTEM_SHORT', r'Central')) \
	.withColumn('1_UNIT_AC', F.regexp('COOLSYSTEM_SHORT', r'1Unit')) \
	.withColumn('2_UNITS_AC', F.regexp('COOLSYSTEM_SHORT', r'2Units')) \
	.withColumn('3_UNITS_AC', F.regexp('COOLSYSTEM_SHORT', r'3Units')) \
	.withColumn('WALL_UNIT_AC', F.regexp('COOLSYSTEM_SHORT', r'WallUnit')) \
	.withColumn('CEILFAN_AC', F.regexp('COOLSYSTEM_SHORT', r'CeilFan')) \
	.withColumn('DUCTLESS_AC', F.regexp('COOLSYSTEM_SHORT', r'Ductless')) \
	.withColumn('MULTIZONE_AC', F.regexp('COOLSYSTEM_SHORT', r'MultiZon')) \
	.withColumn('WOOD_FLOORS', F.regexp('FLOORS_SHORT', r'Wood')) \
	.withColumn('MARBLE_FLOORS', F.regexp('FLOORS_SHORT', r'Marble')) \
	.withColumn('TILE_FLOORS', F.regexp('FLOORS_SHORT', r'Tile')) \
	.withColumn('CARPET_FLOORS', F.regexp('FLOORS_SHORT', r'Carpet')) \
	.withColumn('VINYL_FLOORS', F.regexp('FLOORS_SHORT', r'Vinyl')) \
	.withColumn('LAMINATE_FLOORS', F.regexp('FLOORS_SHORT', r'Laminate')) \
	.withColumn('STONE_FLOORS', F.regexp('FLOORS_SHORT', r'Stone')) \
	.withColumn('PARQUET_FLOORS', F.regexp('FLOORS_SHORT', r'Parquet')) \
	.withColumn('HEAT_SRC_NATGAS', F.regexp('HEAT_SRC_SHORT', r'GasNatur')) \
	.withColumn('HEAT_SRC_ELECTRIC', F.regexp('HEAT_SRC_SHORT', r'Electric')) \
	.withColumn('HEAT_SRC_NATGAS', F.regexp('HEAT_SRC_SHORT', r'GasNatur')) \
	.withColumn('HEAT_SRC_OILABV', F.regexp('HEAT_SRC_SHORT', r'OilAbIn')) \
	.withColumn('HEAT_SRC_OILBEL', F.regexp('HEAT_SRC_SHORT', r'OilBelow')) \
	.withColumn('HEAT_SRC_SOLAR', F.regexp('HEAT_SRC_SHORT', r'SolarLse'))

transformed_df = transformed_df.drop('REMARKSPUBLIC', 'REMARKSAGENT', 'SHOWSPECIAL', 'DRIVEWAYDESC_SHORT',
									 'COOLSYSTEM_SHORT', 'FLOORS_SHORT', 'HEAT_SRC_SHORT', 'HEATSYSTEM_SHORT',
									 'IMAGES')

query = parsed_df.writeStream \
	.foreachBatch(write_to_postgres) \
	.option("checkpointLocation", "/path/to/checkpoint/dir") \
	.start()

# See if I can receive a message from Airflow that tells me the last offset number and terminate the stream after that offset is processed
# if last_offset != airflow_msg:
#     query.awaitTermination()
# else:
query.stop()