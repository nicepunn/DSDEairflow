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
import re
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer, PorterStemmer

import contractions

def lower_case(text):
    """Converts all characters in the text to lowercase."""
    return text.lower()

def expand_contractions(text):
    expanded_text = []
    for word in text.split():
      expanded_text.append(contractions.fix(word))
    expanded_text = ' '.join(expanded_text)
    return expanded_text

def remove_usernames(text):
    """Removes usernames that start with '@'."""
    return re.sub(r'(@\w+)', ' ', text)

def isolate_and_remove_punctuations(text):
    """Isolates and selectively removes punctuations."""
    text = re.sub(r'([\'\"\.\(\)\!\?\\\/\,])', r' \1 ', text)
    text = re.sub(r'[^\w\s\?\%\/\.\-]', ' ', text)
    text = re.sub(r'\©.*?\d{4}\.', ' ', text)
    return text

def remove_numbers(text):
    """Removes numbers from the text."""
    return re.sub(r'\b\d+\.?\d*\b', ' ', text)

def remove_special_characters(text):
    """Removes special characters, preserving some for scientific relevance."""
    return re.sub(r'([\;\:\|•«\n])', ' ', text)

def remove_extra_whitespaces(text):
    """Removes extra whitespaces from the text."""
    return re.sub(r'\s+', ' ', text).strip()

def remove_emojis(text):
    """Removes emojis from the text."""
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def remove_repeated_punctuation(text):
    """Removes repeated punctuation marks."""
    return re.sub(r'([!?.]){2,}', r'\1', text)

def stem_words(text):
    """Applies stemming to each word in the text."""
    stemmer = PorterStemmer()
    token_words = word_tokenize(text)
    stem_sentence = []
    for word in token_words:
        stem_sentence.append(stemmer.stem(word))
        stem_sentence.append(" ")
    return "".join(stem_sentence)

def preserve_scientific_symbols(text):
    """Preserves scientific symbols and notations."""
    # Handling temperatures with spaces, e.g., "30 °C" or "100 °F"
    text = re.sub(r'(\d+)\s*°\s*C\b', r'\1 degrees Celsius', text)
    text = re.sub(r'(\d+)\s*°\s*F\b', r'\1 degrees Fahrenheit', text)

    # Area and Volume
    text = re.sub(r'\bm2/g\b', ' square meters per gram ', text)
    text = re.sub(r'\bcm3/g\b', ' cubic centimeters per gram ', text)

    # Additional notations as previously defined
    text = re.sub(r'\b°C\b', ' degrees Celsius ', text)
    text = re.sub(r'\b°F\b', ' degrees Fahrenheit ', text)
    text = re.sub(r'\bkm/s\b', ' kilometers per second ', text)
    text = re.sub(r'\bmg/L\b', ' milligrams per liter ', text)
    text = re.sub(r'\bPa\b', ' Pascals ', text)
    text = re.sub(r'\bkPa\b', ' kilopascals ', text)
    text = re.sub(r'\bMPa\b', ' megapascals ', text)
    text = re.sub(r'\bbar\b', ' bars ', text)
    text = re.sub(r'\bJ\b', ' Joules ', text)
    text = re.sub(r'\bkJ\b', ' kilojoules ', text)
    text = re.sub(r'\bMJ\b', ' megajoules ', text)
    text = re.sub(r'\bcal\b', ' calories ', text)
    text = re.sub(r'\bkcal\b', ' kilocalories ', text)
    text = re.sub(r'\bL/min\b', ' liters per minute ', text)
    text = re.sub(r'\bmL/s\b', ' milliliters per second ', text)
    text = re.sub(r'\bkg\b', ' kilograms ', text)
    text = re.sub(r'\bg\b', ' grams ', text)
    text = re.sub(r'\bmg\b', ' milligrams ', text)
    text = re.sub(r'\bkm\b', ' kilometers ', text)
    text = re.sub(r'\bm\b', ' meters ', text)
    text = re.sub(r'\bcm\b', ' centimeters ', text)
    text = re.sub(r'\bmm\b', ' millimeters ', text)

    return text


def text_preprocessing(s, remove_numbers_flag=False):
    s = preserve_scientific_symbols(s)  # Preserve scientific symbols before removing numbers
    s = lower_case(s)
    s = expand_contractions(s)
    s = remove_usernames(s)
    s = isolate_and_remove_punctuations(s)
    s = remove_emojis(s)
    s = remove_repeated_punctuation(s)
    if remove_numbers_flag: s = remove_numbers(s)
    s = remove_special_characters(s)
    s = remove_extra_whitespaces(s)
    return s

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

years = [2018,2019,2020,2021,2022,2023] #


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
    startIndex = 0
    page_param = f'start={startIndex}&count=25&' # max count 25 entity
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
                rawkeywords = data["abstracts-retrieval-response"]["authkeywords"]["author-keyword"]
                keywords = [text_preprocessing(d["$"]) for d in eval(str(rawkeywords))] if type(rawkeywords) == list else [text_preprocessing(d["$"]) for d in eval(str([rawkeywords]))]
                data_list.append({
                    "Article Id": int(article_id),
                    "Title": title,
                    "Abstract": abstract,
                    "Publish Date": publish_date,
                    "Keywords": keywords
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