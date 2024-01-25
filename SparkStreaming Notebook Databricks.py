# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime as dt
import json

connectionString = "Endpoint=sb://eh-namespace-stream.servicebus.windows.net/;SharedAccessKeyName=ve-policy;SharedAccessKey=RAjOTKnsI8CrdWbrR/hLi+yyOjZozPsUy+AEhLX586c=;EntityPath=eh-streamproject"
ehConf = {}
ehConf["eventhubs.connectionString"] = connectionString
ehConf["eventhubs.consumerGroup"] = "$Default"

json_schema = StructType(
    [
        StructField("deviceID", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("angle", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("windspeed", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("deviceTimestamp", StringType(), True),
        StructField("deviceDate", StringType(), True),
    ]
)
