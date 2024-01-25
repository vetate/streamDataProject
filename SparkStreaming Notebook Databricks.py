# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

try:
    spark.sql("create catalog streaming;")
except:
    print('check if catalog already exists')

try:
    spark.sql("create schema streaming.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("create schema streaming.silver")
except:
    print('check if silver schema already exists')

try:
    spark.sql("create schema streaming.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------

try:
    spark.sql("create catalog streaming;")
except:
    print('check if catalog already exists')

try:
    spark.sql("create schema streaming.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("create schema streaming.silver")
except:
    print('check if silver schema already exists')

try:
    spark.sql("create schema streaming.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------


# Config
# Replace with your Event Hub namespace, name, and key
connectionString = "Endpoint=sb://eh-namespace-stream.servicebus.windows.net/;SharedAccessKeyName=ve-policy;SharedAccessKey=RAjOTKnsI8CrdWbrR/hLi+yyOjZozPsUy+AEhLX586c=;EntityPath=eh-streamproject"
eventHubName = "eh-streamproject"

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}
   

# COMMAND ----------


# Reading stream: Load data from Azure Event Hub into DataFrame 'df' using the previously configured settings
df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load() \

# Displaying stream: Show the incoming streaming data for visualization and debugging purposes
df.display()

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://streaming@moviesdatasa.blob.core.windows.net',
    mount_point='/mnt/streaming',
    extra_configs = {'fs.azure.account.key.moviesdatasa.blob.core.windows.net': dbutils.secrets.get('moviesScopeProject', 'storageAccountKey')}
)

# COMMAND ----------

# Writing stream: Persist the streaming data to a Delta table 'streaming.bronze.weather' in 'append' mode with checkpointing
df.writeStream\
    .option("checkpointLocation", "/mnt/streaming/bronze/weather")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streaming.bronze.weather")
