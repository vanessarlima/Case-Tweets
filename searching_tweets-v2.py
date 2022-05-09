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
id = []
created_at = []
users = []
tweets = []
for tweet in results:
    id.append(tweet.id)
    created_at.append(tweet.created_at)
    users.append(tweet.user.name)
    tweets.append(tweet.text)
    print(f'id: {tweet.id} | User: {tweet.user.name} | Tweet: {tweet.text} | Tweet: {tweet.created_at}')

# COMMAND ----------

# Montando um dataset com os tweets extraidos do Twitter
df_tweets = pd.DataFrame()
df_tweets['id'] = id
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
# MAGIC SELECT * FROM tv_tweets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS tweets.search_tweets
# MAGIC USING DELTA
# MAGIC --LOCATION ''
# MAGIC AS
# MAGIC SELECT * FROM tv_tweets

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tweets.search_tweets i
# MAGIC USING (SELECT * FROM tv_tweets) x
# MAGIC ON x.id = i.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
