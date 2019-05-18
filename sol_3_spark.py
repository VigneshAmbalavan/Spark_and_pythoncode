
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

total_session_day=spark.sql("select count(distinct session_id) from gold.click_stream_activity_detail group by source_date")
total_session_day.show()
### as a action is required to run a spark program otherwise we will get no output.that's why show()####


total_time_user_day=spark.sql("select sum(total_activity_time) from gold.click_stream_activity_detail group by user_id,source_date")
total_time_user_day.show()

total_time_user_monthly=spark.sql("select sum(total_activity_time) from gold.click_stream_activity_detail where source_date between '%s' and '%s' group by user_id"%(date_month_start,date_end_month))
total_time_user_monthly.show()
