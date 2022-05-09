# Databricks notebook source
twitter_key = dbutils.secrets.get(scope = "twitter", key = "bearrer_token_twitter")

# COMMAND ----------

consumer_key_twitter = dbutils.secrets.get(scope = "twitter", key = "consumer_key_twitter")

# COMMAND ----------

consumer_secret_twitter = dbutils.secrets.get(scope = "twitter", key = "consumer_secret_twitter")

# COMMAND ----------

access_token_twitter = dbutils.secrets.get(scope = "twitter", key = "access_token_twitter")

# COMMAND ----------

access_token_secret_twitter = dbutils.secrets.get(scope = "twitter", key = "access_token_secret_twitter")
