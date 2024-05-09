print("######################################")
print("in old-scraping-spark")
print("######################################")


import datetime
import sys
import os
import json
import requests
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


api_resource = "https://api.elsevier.com/content/search/scopus?"

startIndex = 0
years = [2018,2019,2020,2021,2022,2023] #
page_param = f'start={startIndex}&count=25&' # max count 25 entity


api_key='f1a815f797828a83a6eb76d7314d3dab'

dir="/usr/local/spark/assets/data/output_scraping" # directory to save data

if not os.path.exists(dir):
    os.makedirs(dir)

data_list = []

# headers
headers = dict()
headers['X-ELS-APIKey'] = api_key
headers['X-ELS-ResourceVersion'] = 'XOCS'
headers['Accept'] = 'application/json'

for year in years :
    search_param = f'query=engineering+AND+PUBYEAR+=+{year}&sort=citedby-count'


    # request with first searching page
    page_request = requests.get(api_resource + page_param + search_param, headers=headers)


    # response to json
    page = json.loads(page_request.content.decode("utf-8"))

    total = int(page['search-results']['opensearch:totalResults'])

    while startIndex !=5000 and startIndex < total : #result limit 5000 entity
        # List of articles from this page
        # try:
        articles_list = page['search-results']['entry']

        for idx,article in enumerate(articles_list):
            article_url = article['prism:url']

            article_request = requests.get(article_url, headers=headers)
            article_content = json.loads(article_request.content.decode("utf-8"))
            article_id = article_url.split('/')[-1]
            
            file_name = str(year)+'{:04}'.format(idx+startIndex)

            # Writing article content to a JSON file
            # with open(f"{dir}/{file_name}.json", "w") as json_file:
            #     json.dump(article_content, json_file)
            
            # os.chmod(f"{dir}/{file_name}.json", 0o777)
            data = article_content

            if data["abstracts-retrieval-response"]["authkeywords"] != None :
                abstract = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["abstracts"]
                title = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-title"]
                publish_date = pd.to_datetime(data["abstracts-retrieval-response"]["item"]["ait:process-info"]["ait:date-delivered"]["@timestamp"]).strftime('%Y-%m-%d')
                keywords = data["abstracts-retrieval-response"]["authkeywords"]["author-keyword"]

                data_list.append({
                    "Article Id": article_id,
                    "Title": title,
                    "Abstract": abstract,
                    "Publish Date": publish_date,
                    "RawKeywords": keywords
                })
            if len(data_list) > 10:
                break
        if len(data_list) > 10:
            break

        startIndex += 25
        page_param = 'start={startIndex}&count=25&'

        page_request = requests.get(api_resource + page_param + search_param, headers=headers)

        page = json.loads(page_request.content.decode("utf-8"))

        # except:
        #     print(api_resource + page_param + search_param)
        #     print(page)
        #     break

df = pd.DataFrame(data_list)
df.to_csv(f"{dir}/scrap.csv", index = False, encoding="utf-8")