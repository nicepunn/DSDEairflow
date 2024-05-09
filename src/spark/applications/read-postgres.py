import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
spark = (SparkSession
         .builder
         .getOrCreate()
         )

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

df_all = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.all")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

df_all = df_all.alias("a")
# df_result = (
#     df_join
#     .groupBy("title")
#     .agg(
#         F.count("timestamp").alias("qty_ratings"), F.mean(
#             "rating").alias("avg_rating")
#     )
#     .sort(F.desc("qty_ratings"))
#     .limit(10)
# )

df_result = df_all

print("######################################")
print("EXECUTING QUERY AND SAVING RESULTS")
print("######################################")

df_result.coalesce(1).write.format("csv").mode("overwrite").save(
    "/usr/local/spark/assets/data/output_postgres", header=True)
