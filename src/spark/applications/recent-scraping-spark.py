print("######################################")
print("in recent-scraping-spark")
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

class dfType:
    def __init__(self,title:str,abstract:str,publishDate:str,keyword:list):
        self.title =title
        self.abstract =abstract
        self.publishDate =publishDate
        self.keyword =keyword
    
    def to_dict(self):
        return {
            'Title': self.title ,
            'Abstract':self.abstract ,
            'Publish Date':self.publishDate ,
            'Keywords':self.keyword 
        }
        
def checkDateFormat(date_string:str)->str:
    formats_to_try = ['%d %B %Y', '%B %d, %Y']
    
    for date_format in formats_to_try:
        try:
            datetime.strptime(date_string, date_format)
            return True
        except ValueError:
            pass
    
    return False

def changeDateFormat(date_str:str):
    # Attempt to parse the date string using different formats
    for fmt in ["%d %B %Y", "%B %d, %Y"]:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            # If successfully parsed, format the date as YYYY-MM-DD
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # If no valid format found, return None or handle as needed
    return None

api_resource = "https://api.elsevier.com/content/search/scopus?"

startIndex = 0
page_param = 'start={startIndex}&count=25&' # max count 25 entity
search_param = 'query=engineering'  # for example


api_resource = "https://api.elsevier.com/content/search/scopus?"

startIndex = 0
page_param = 'start={startIndex}&count=25&' # max count 25 entity
search_param = 'query=engineering'  # for example


api_key='23859eabdf79de656745432b30f2122a'

dir="/usr/local/spark/assets/data/output_scraping" # directory to save data
# dir = "data"

if not os.path.exists(dir):
    os.makedirs(dir)

# headers
headers = dict()
headers['X-ELS-APIKey'] = api_key
headers['X-ELS-ResourceVersion'] = 'XOCS'
headers['Accept'] = 'application/json'

# request with first searching page
page_request = requests.get(api_resource + page_param + search_param, headers=headers)

# response to json
page = json.loads(page_request.content.decode("utf-8"))

total = int(page['search-results']['opensearch:totalResults'])

data_list = []

while startIndex !=5000 and startIndex < total : #result limit 5000 entity
    # List of articles from this page
    articles_list = page['search-results']['entry']

    for article in articles_list:
        article_url = article['prism:url']

        article_request = requests.get(article_url, headers=headers)
        article_content = json.loads(article_request.content.decode("utf-8"))
        article_id = article_url.split('/')[-1]

        #Writing article content to a JSON file
        # with open(f"{dir}/{article_id}.json", "w") as json_file:
        #     json.dump(article_content, json_file)
        # os.chmod(f"{dir}/{article_id}.json", 0o777)

        data = article_content

        if data["abstracts-retrieval-response"]["authkeywords"] != None :
          abstract = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["abstracts"]
          title = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-title"]
          publish_date = pd.to_datetime(data["abstracts-retrieval-response"]["item"]["ait:process-info"]["ait:date-delivered"]["@timestamp"]).strftime('%Y-%m-%d')
          keywords = data["abstracts-retrieval-response"]["authkeywords"]["author-keyword"]

          data_list.append({
              "Article Id": int(article_id),
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

df = pd.DataFrame(data_list)
df.to_csv(f"{dir}/scrap.csv", index = False, encoding="utf-8")


