# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests
import json
from pyspark.sql import *
import pandas as pd

# COMMAND ----------

# MAGIC %run Users/vanessa.rogeria.lima_outlook.com#ext#@vanessarogerialimaoutlook.onmicrosoft.com/Util/twitter_connection

# COMMAND ----------

#to recover bearer token
BEARER_TOKEN = twitter_key

# COMMAND ----------

#define search twitter function
def search_twitter(query, tweet_fields, bearer_token = BEARER_TOKEN):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}".format(
        query, tweet_fields
    )
    response = requests.request("GET", url, headers=headers)

    print(response.status_code)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# COMMAND ----------

#search term
query = "eleicao"

#twitter fields to be returned by api call
tweet_fields = "tweet.fields=text,author_id,created_at"

#twitter api call
json_response = search_twitter(query=query, tweet_fields=tweet_fields, bearer_token=BEARER_TOKEN)

#pretty printing
print(json.dumps(json_response, indent=10, sort_keys=True))

# COMMAND ----------

df = pd.DataFrame.from_dict(json_response, orient="index")
print(df)
