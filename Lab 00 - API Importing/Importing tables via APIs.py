# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Importing tables via APIs
# MAGIC 
# MAGIC **APIs seem to be the leading methodology for data importing from systems such as Guidewire.**

# COMMAND ----------

base_table_path = "dbfs:/FileStore/QBE/"
local_data_path = "/dbfs/FileStore/QBE/"

# COMMAND ----------

# MAGIC %md
# MAGIC **URL APIs**
# MAGIC 
# MAGIC CSV Format

# COMMAND ----------

import http
import csv
import requests

authkey = 'BXD67GMINQA6W5JB' #This is the authetication key for our API - we are using AlphaVantage to track stock prices
ticker = 'MSFT' #Which company do we want to look for?
function = 'TIME_SERIES_INTRADAY_EXTENDED' # see docs
interval = '1min' # see docs

# COMMAND ----------

url = f'https://www.alphavantage.co/query?function={function}&symbol={ticker}&interval={interval}&apikey={authkey}'

with requests.Session() as s:
    download = s.get(url)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
  

# COMMAND ----------

df = spark.createDataFrame(my_list[1:], my_list[0])
df.display()

# COMMAND ----------

df.write.mode("overwrite").save(f"{base_table_path}/data1")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*)
# MAGIC FROM delta.`dbfs:/FileStore/QBE/data1`

# COMMAND ----------


