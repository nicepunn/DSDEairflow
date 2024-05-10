# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_unixtime, col, to_timestamp
# from pyspark.sql.types import DoubleType

# # Create spark session
# spark = (SparkSession
#          .builder
#          .getOrCreate()
#          )

# ####################################
# # Parameters
# ####################################

# file_to_load = sys.argv[1]
# postgres_db = sys.argv[2]
# postgres_user = sys.argv[3]
# postgres_pwd = sys.argv[4]

# ####################################
# # Read CSV Data
# ####################################
# print("######################################")
# print("READING CSV FILES")
# print("######################################")

# df_to_load = (
#     spark.read
#     .format("csv")
#     .option("header", True)
#     .load(file_to_load)
# )

# ####################################
# # Load data to Postgres (with deduplication)
# ####################################
# print("######################################")
# print("LOADING POSTGRES TABLES")
# print("######################################")

# df_postgres = (
#     spark.read
#     .format("jdbc")
#     .option("url", postgres_db)
#     .option("dbtable", "public.all")
#     .option("user", postgres_user)
#     .option("password", postgres_pwd)
#     .load()
# )

# print("read from postgres")
# df_postgres.show()

# df_mod = df_to_load.union(df_postgres).dropDuplicates()
# df_all_csv_fmt = (
#     df_mod
# )

# print("all to load")
# df_all_csv_fmt.show()

# (
#     df_postgres
#     .write
#     .format("jdbc")
#     .option("url", postgres_db)
#     .option("dbtable", "public.all")
#     .option("user", postgres_user)
#     .option("password", postgres_pwd)
#     .mode("overwrite")
#     .save()
# )



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
    .dropDuplicates(['Article Id'])
    .write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.all")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("append")
    .save()
)