import sys
import os
import json
import pandas as pd
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

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# json to CSV // uncommented this if no CSV
####################################

# data_list = []
# json_folder = "/usr/local/spark/assets/data/init_data_json"
# c = 0
# for filename in os.listdir(json_folder):
#     c += 1
#     file_path = os.path.join(json_folder, filename)
#     with open(file_path) as f:
#         data = json.load(f)
#         if data["abstracts-retrieval-response"]["authkeywords"] != None :
#             abstract = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["abstracts"]
#             title = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-title"]
#             publish_date = pd.to_datetime(data["abstracts-retrieval-response"]["item"]["ait:process-info"]["ait:date-delivered"]["@timestamp"]).strftime('%Y-%m-%d')
#             keywords = data["abstracts-retrieval-response"]["authkeywords"]["author-keyword"]
#             ID = 0
#             for item in data['abstracts-retrieval-response']['item']['bibrecord']['item-info']["itemidlist"]["itemid"]:
#                 if item["@idtype"] == "SCP":
#                     ID = int(item["$"])
#                     break
#             # print(ID)

#             data_list.append({
#                     "Article Id": ID,
#                     "Title": title,
#                     "Abstract": abstract,
#                     "Publish Date": publish_date,
#                     "RawKeywords": keywords
#             })
        
#     # print(c)
        

# df = pd.DataFrame(data_list)

# df.to_csv("/usr/local/spark/assets/data/init_data.csv", index = False, encoding="utf-8")
# print("to csv finished")

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
    .load("/usr/local/spark/assets/data/init_data.csv")
)

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
