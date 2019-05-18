#!/usr/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import csv
import re
import os
from os import listdir
from subprocess import call
from datetime import datetime
import ConfigParser
import argparse
import shutil


start_time=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

spark = SparkSession.builder.enableHiveSupport().config("hive.exec.dynamic.partition.mode", "nonstrict").\
    config("hive.exec.dynamic.partition", "true").config("spark.locality.wait.node", "0").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#####reading from the hive table##########

df=spark.table('incoming_clickstream_agam')

#####repartioning on USER_ID so that same uhid get into same partition and then doing sort with in each partition on logtime#####

df2=df.repartition('user_id').sortWithinPartitions('logtime')

#####creating a tempview#####

df2.createOrReplaceTempView('clickstream_sorted')

#####calculating the time_difference between the active sorted logtime group over every user_id#########

df3=spark.sql("SELECT user_id,coalesce((UNIX_TIMESTAMP(logtime) - LAG (UNIX_TIMESTAMP(logtime))  OVER (PARTITION BY user_id ORDER BY logtime)),0) as total_activity_time,logtime,source_date  FROM clickstream_sorted").createOrReplaceTempView('clickstream_sorted_new')

######creating final transformed data having inplace logic for counting a new session#########

final_df=spark.sql("select user_id,CONCAT(user_id,CONCAT('_',SUM(new_session) OVER (PARTITION BY user_id ORDER BY logtime))) as session_id,logtime,total_activity_time,source_date from(SELECT *, CASE WHEN total_activity_time >= 30 * 60   THEN 1  ELSE 0  END AS new_session,source_date FROM clickstream_sorted_new) s1").createOrReplaceTempView('gold_clickstream_agam')

######finally writing to the gold parquet table partition on source_date#########
final_result=spark.sql("insert overwrite table gold.click_stream_activity_detail partition(source_date) select * from gold_clickstream_agam")
