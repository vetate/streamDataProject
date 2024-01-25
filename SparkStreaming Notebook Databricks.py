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


# Config
# Replace with your Event Hub namespace, name, and key
connectionString = "Endpoint=sb://eh-namespace-stream.servicebus.windows.net/;SharedAccessKeyName=ve-policy;SharedAccessKey=RAjOTKnsI8CrdWbrR/hLi+yyOjZozPsUy+AEhLX586c=;EntityPath=eh-streamproject"
eventHubName = "eh-streamproject"

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}
   

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://streaming@moviesdatasa.blob.core.windows.net',
    mount_point='/mnt/streaming',
    extra_configs = {'fs.azure.account.key.moviesdatasa.blob.core.windows.net': dbutils.secrets.get('moviesScopeProject', 'storageAccountKey')}
)

# COMMAND ----------

dbutils.fs.chmod(
    "/mnt/streaming/streaming.bronze/",  # Path to the directory
    "775"  # Octal notation for permissions (user: read, write, execute; group: read, write, execute; others: read, execute)
)

# COMMAND ----------

# Reading stream: Load data from Azure Event Hub into DataFrame 'df' using the previously configured settings
df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Displaying stream: Show the incoming streaming data for visualization and debugging purposes
df.display()

# Write stream to Hive table in Parquet format
checkpoint_location = "/mnt/streaming/checkpoints/"
query_name = "streaming_query"


# Write stream to Hive table in Parquet format
df.writeStream \
    .format("parquet") \
    .option("path", "/mnt/streaming/streaming.bronze/") \
    .option("checkpointLocation", "/mnt/streaming/checkpoints/") \
    .trigger(processingTime="60 seconds") \
    .start() \
    .awaitTermination()
