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

inputs = sys.argv[1:]
print ('length of input '+str(len(inputs)))

try:
    if len(inputs) == 3:
       file_name  = str(inputs[0])
       latest_date_partition = str(inputs[1])
       py_config_file = str(inputs[2])
    else:
        print ('cus_onboarding_ip_matching_load_extract_tos3.py <file_name>')
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        sys.exit(1)

except Exception as e:
    print ('exception occured', e)
    print ('cus_onboarding_ip_matching_load_extract_tos3.py <file_name>')
    print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
    raise e
    sys.exit(1)
config = ConfigParser.RawConfigParser()
try:
        config.read(py_config_file)
        s3_credential_file = config.get('Sectionextract','S3_CREDENTIAL_FILE')
        filepath = config.get('Sectionextract','EXTRACT_FILE_PATH')
        s3_extract_location = config.get('Sectionextract','S3_EXTRACT_LOCATION')
except IOError as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' could not read the file to get hcp and common details')
        print ("Following exception occurred while reading the config file ", e)
        raise e
        sys.exit(1)

spark = SparkSession.builder.enableHiveSupport().config("hive.exec.dynamic.partition.mode", "nonstrict").\
    config("hive.exec.dynamic.partition", "true").config("spark.locality.wait.node", "0").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sql("create temporary function haship AS 'com.alticeusa.apcompiler.udf.hashIP'")
segment_name=file_name.split("_")[0]
advertiser_name=file_name.split("_")[1]

hash_uhash_ip_mapping_DF = spark.sql("select *  from work_ntelligis.hash_unhash_ip_mapping where source_date='%s' limit 10"%(latest_date_partition))
if len(hash_uhash_ip_mapping_DF.take(1))==0:
   print("for source_date as {0} , no mapping hashed_unhashed_ip is present,so will have to create".format(latest_date_partition))
   df4=spark.sql("select distinct ip from incoming_ntelligis.segment_ip_mapping where source_date='%s'"%(latest_date_partition)).createOrReplaceTempView("segment_ip")
   try:
       spark.sql("select haship(ip) as hash_ip_addr,ip,'%s' as source_date from segment_ip"%(latest_date_partition)).write.mode("Overwrite").saveAsTable("work_ntelligis.hash_unhash_ip_mapping")
   except Exception as e:
           print("ERROR: Unable to write work_ntelligis.hash_unhash_ip_mapping table for source_date as {0}".format(latest_date_partition))
           print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
           raise e
           sys.exit(1)
else:
   print("for source_date as {0} , mapping of hashed_unhashed_ip is already present".format(latest_date_partition))
   
def rename_del_from_s3(outgoing_hdfs,file_name,segment_name,s3_credential_file,s3_extract_location,extract_type,advertiser_name):
     print("renaming the {0} file as input_file_name as {1} for segment_name as {2},advertiser_name as {3}".format(extract_type,file_name,segment_name,advertiser_name))
     if not call(["hdfs","dfs","-mv",outgoing_hdfs+"/part*.csv.gz",outgoing_hdfs+"/%s.csv.gz"%file_name]):
        print("successfully renamed the part file to {0}.csv.gz".format(file_name))
     else:
        print("not able to rename the part fie to {0}.csv.gz".format(file_name))
        sys.exit(1)
     print("Removing the file {0} if it exists on S3 extract location".format(file_name))
     if not call(["hdfs","dfs","-Dhadoop.security.credential.provider.path=%s"%s3_credential_file,"-rm","-r","-f",s3_extract_location+"/%s.csv.gz"%file_name]):
        print("successfully deleted the file from s3 if it is present there for file as {0}".format(file_name))
     else:
        print("not able to delete the filr from the s3 for file as {0}".format(file_name))
        sys.exit(1)

print("process of loading extract to s3 for %s as segment_name partition and %s as advertiser_name for file: %s"%(segment_name,advertiser_name,file_name))

print("starting loading extract of segment name "+segment_name+",advertiser_name as"+ advertiser_name+ "and file name "+file_name+" to s3")
try:
     extract_DF = spark.sql("select nu_uhid,uhid,hash_ip_addr  from gold_ntelligis.custom_onboarding_ip_matching  where segment_name='%s' and advertiser_name='%s' and file_name='%s'"%(segment_name,advertiser_name,file_name)).cache()
     print("preparing extract for nu_uhid for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     extract_DF.filter(length('nu_uhid')>2).select("nu_uhid").coalesce(1).write.csv(filepath, mode='append', compression='gzip', header='false')
     print("nu_uhid extract successfully completed for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     rename_del_from_s3(filepath,file_name,segment_name,s3_credential_file,s3_extract_location,'nu_uhid',advertiser_name)
except Exception as e:
     print("ERROR: Unable to write extract nu_uhid to HDFS file for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
     raise e 
     sys.exit(1)
try:
     print("preparing extract for uhid for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     extract_DF.filter(length('uhid')>2).filter((length('nu_uhid')>2) | (length('hash_ip_addr')>2)).select("uhid").coalesce(1).write.csv(filepath, mode='append', compression='gzip', header='false')
     print("uhid extract successfully completed for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     uhid_file_name='uh_'+file_name
     rename_del_from_s3(filepath,uhid_file_name,segment_name,s3_credential_file,s3_extract_location,'uhid',advertiser_name)
except Exception as e:
     print ("ERROR: Unable to write extract uhid to HDFS file for file_name as {0} and segment_name as {1} and advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
     raise e
     sys.exit(1)
try:
     print("preparing extract for unhashed ip_addresses  for file_name as {0} and segment_name as {1} and advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     check_ip_df=extract_DF.filter(length('hash_ip_addr')>2).select("hash_ip_addr")
     if len(check_ip_df.take(1))==0:
         print("no ip got selected for file {0} and segment{1},advertiser_name as {2},so no join to get unhash ip".format(file_name,segment_name,advertiser_name))
         check_ip_df.coalesce(1).write.csv(filepath, mode='append', compression='gzip', header='false')
     else:
         check_ip_df.createOrReplaceTempView("extracted_uhid_tbl")
         unhash_ip_extract_DF=spark.sql("select b.ip as unhash_ip_addr from extracted_uhid_tbl a inner join work_ntelligis.hash_unhash_ip_mapping b on a.hash_ip_addr=b.hash_ip_addr")
         unhash_ip_extract_DF.coalesce(1).write.csv(filepath, mode='append', compression='gzip', header='false')
         print("unhased ip  extract successfully completed for file_name as {0} and segment_name as {1},advertiser_name as {2}".format(file_name,segment_name,advertiser_name))
     ip_file_name='ip_'+file_name
     rename_del_from_s3(filepath,ip_file_name,segment_name,s3_credential_file,s3_extract_location,'ip',advertiser_name)
except Exception as e:
         print ("ERROR: Unable to write extract to hdfs for file_name as {0} and segment_name as {1}".format(file_name,segment_name))
         print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
         raise e
         sys.exit(1)
try:
     print("starting the loading of newly created all  extract files to s3 in one go")
     if not call(["hadoop","distcp","-Dhadoop.security.credential.provider.path=%s"%s3_credential_file,filepath+"/*.csv.gz",s3_extract_location]):
        print("Sucessfully loaded the files to the S3 extract bucket")
     else:
        print("failed in loading the extract files to the s3 extract bucket")
        sys.exit(1)
except Exception as e:
     print ("ERROR: Unable to copy the extract to s3 extract folder")
     print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
     raise e
print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " INFO: Wrote DF to HDFS File")
