import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType

# Create spark session
spark = (SparkSession
         .builder
         .getOrCreate()
         )

####################################
# Parameters
####################################

all_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")

df_all_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(all_file)
)

# Convert epoch to timestamp and rating to DoubleType
df_all_csv_fmt = (
    df_all_csv
)

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    df_all_csv_fmt
    .select([c for c in df_all_csv_fmt.columns if c != "timestamp_epoch"])
    .write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.all")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)
