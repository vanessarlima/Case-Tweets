# Databricks notebook source
# MAGIC %pip install tweepy

# COMMAND ----------

import tweepy as tw
import json
import pandas as pd
import pyspark
from pyspark.sql import *

# COMMAND ----------

# MAGIC %run Users/vanessa.rogeria.lima_outlook.com#ext#@vanessarogerialimaoutlook.onmicrosoft.com/Util/twitter_connection

# COMMAND ----------

consumer_key = consumer_key_twitter
consumer_secret = consumer_secret_twitter
access_token = access_token_twitter
access_token_secret = access_token_secret_twitter

# COMMAND ----------

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# COMMAND ----------

# definicao das variaveis
search_words = "#covid19" + "-filter:retweets"

# COMMAND ----------

# Realizando uma busca de dados pelo Top Trend
results = api.search_tweets(q=search_words, lang="pt")
 
# Organizando os dados
created_at = []
users = []
tweets = []
for tweet in results:
    created_at.append(tweet.created_at)
    users.append(tweet.user.name)
    tweets.append(tweet.text)
    print(f'User: {tweet.user.name} | Tweet: {tweet.text} | Tweet: {tweet.created_at}')

# COMMAND ----------

# Montando um dataset com os tweets extraidos do Twitter
df_tweets = pd.DataFrame()
df_tweets['created_at'] = created_at
df_tweets['users'] = users
df_tweets['tweets'] = tweets
df_tweets.head()

# COMMAND ----------

df = spark.createDataFrame(df_tweets)

# COMMAND ----------

df.createOrReplaceTempView("tv_tweets")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tweets.search_tweets
# MAGIC USING DELTA
# MAGIC --LOCATION ''
# MAGIC AS
# MAGIC SELECT * FROM tv_tweets

# COMMAND ----------

#print do json
tweet._json

# COMMAND ----------

#prin das chaves
twkeys = tweet._json.keys()
twkeys

# COMMAND ----------

#atribuindo as chaves no dicionario
tweets_dict = {}
tweets_dict = tweets_dict.fromkeys(twkeys)
tweets_dict

# COMMAND ----------

cursor_tweets = api.search_tweets(q=search_words, lang="pt")

# COMMAND ----------

for tweet in cursor_tweets:
    for key in tweets_dict.keys():
        try:
            twvalue = tweet._json[key]
            tweets_dict[key].append(twvalue)
        except KeyError:
            twvalue = ""
            if(tweets_dict[key] is None):
                tweets_dict[key] = [twvalue]
            else:
                tweets_dict[key].append(twvalue)
        except:
            tweets_dict[key] = [twvalue]
        print("tweets_dict[key]: {} - tweet[key]: {}".format(tweets_dict[key],  twvalue))

# COMMAND ----------

dfTweets = pd.DataFrame.from_dict(tweets_dict)

# COMMAND ----------

dfTweets.head()

# COMMAND ----------

df = spark.createDataFrame(dfTweets)

# COMMAND ----------

df.createOrReplaceTempView("tv_tweets_v2")

# COMMAND ----------

pd.to_parquet(dfTweets)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tweets.tv_tweets_v2
# MAGIC USING DELTA
# MAGIC --LOCATION ''
# MAGIC AS
# MAGIC SELECT * FROM tv_tweets_v2
