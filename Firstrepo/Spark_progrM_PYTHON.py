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
#from email_consumers_new import emailConsumers
import ConfigParser
import argparse
import shutil


start_time=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

inputs = sys.argv[1:]
print ('length of input'+str(len(inputs)))

try:
    if len(inputs) == 2:
       file_name  = str(inputs[0])
       num_col = int(inputs[1])
       
    else:
        print ('cus_onboard_load_to_incoming.py <file_name>')
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        sys.exit(1)

except Exception as e:
    print ('exception occured')
    print ('cus_onboard_load_to_gold.py <file_name>')
    print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
    raise e
    sys.exit(1)

config = ConfigParser.RawConfigParser()
try:
        config.read('pg.ini')
        std_address_master_db = config.get('SectionUhid','STD_ADDRESS_MASTER_DB')
        std_address_master_tbl = config.get('SectionUhid','STD_ADDRESS_MASTER_TBL')
        decryption_user = config.get('SectionUhid','DECRYPTION_USER')
        dec_nn_service = config.get('SectionUhid','DEC_NN_SERVICE')
        #unarchive_dir = config.get('SectionUhid','UNARCHIVE_DIR_PATH')
        incoming_database = config.get('SectionUhid','INCOMING_DATABASE')
        gold_database = config.get('SectionUhid','GOLD_DATABASE')
        incoming_table = config.get('SectionUhid','INCOMING_TABLE')
        gold_table  = config.get('SectionUhid','GOLD_TABLE')
        gold_invalid_tbl = config.get('SectionUhid','gold_invalid_tbl')
except IOError as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' could not read the file to get hcp and common details')
        print ("Following exception occurred while reading the config file ", e)
        raise e
        sys.exit(1)

spark = SparkSession.builder.enableHiveSupport().config("hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.dynamic.partition", "true").config("spark.locality.wait.node", "0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def partition_count(splitIndex,iterator):
        count=0
        for x in iterator:
                count+=1
        yield splitIndex,count
		
spark.sql("create temporary function standardize_batch_addresses as 'com.alticeusa.audiencepartners.udf.StandardizeBatchAddresses'")

spark.sql("create temporary function edpdecrypt AS 'com.alticeusa.edp.udf.EDPHiveDecryptUDF'")
spark.sql("create temporary function edpEncrypt AS 'com.alticeusa.edp.udf.EDPHiveEncryptUDF'")

segment_name=file_name.split("_")[0]
advertiser_name=file_name.split("_")[1]
print("process of loading gold table starting for %s as segment_name partition and %s as advertiser_name for file: %s"%(segment_name,advertiser_name,file_name))
if num_col==3:
   print("file has four columns i.e address columns,so going for standardizing process")
   check_df=spark.sql("select  lower(trim(address)) as input_address,COALESCE(lower(trim(city)),'') as input_city, COALESCE(lower(trim(state)),'') as input_state, COALESCE(lower(trim(zip)),'') as input_zip  from incoming_ntelligis.custom_onboarding_ip_matching where segment_name='%s' and advertiser_name='%s'"%(segment_name,advertiser_name))

   new_df=check_df.repartition(100,'input_address').withColumn("partition_id", spark_partition_id())

   count_df = new_df.rdd.mapPartitionsWithIndex(partition_count).toDF().withColumnRenamed("_1", "partition_id").withColumnRenamed("_2", "count")
	  
   df_w_prtn_cnt=new_df.join(broadcast(count_df),"partition_id")
	  
   df_w_prtn_cnt.createOrReplaceTempView("src_addr")


   try:	  
       out_df = spark.sql("select inline(standardize_batch_addresses(input_address,'',input_zip,input_city,input_state,100, count)) from src_addr")
   except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("Error while getting standardize addresses via calling standard_address UDF for segment_name as %s and advertiser_name as %s for file:%s"%(segment_name,advertiser_name,file_name))
        raise e
        sys.exit(1)

   out_df.cache().createOrReplaceTempView("standardize_out1")
   out_df1=spark.sql("select input_street as src_street,input_zip as src_zipcode, input_city as src_city, input_state as src_state,std_delivery_line_1,std_delivery_line_2,std_zipcode,std_city,std_state from standardize_out1 where std_delivery_line_1 is not null  and std_zipcode is not null and std_city is not null and std_state is not null")
   
   invalid_addr_non_standardized_df=spark.sql("select input_street as src_street,input_zip as src_zipcode, input_city as src_city, input_state as src_state  from standardize_out1 where std_delivery_line_1 is  null  and std_zipcode is  null and std_city is  null and std_state is null")
   
   print("writing the invalid addresses to the partitioned invalid uhid_ip_mapping table")
   
   try: 
        invalid_addr_non_standardized_df.createOrReplaceTempView("invalid_addr_non_standardized_output")
        spark.sql("insert into table %s.%s   select src_street,src_zipcode,src_city,src_state,'NON_STANDARDIZED_ADDRESS','%s','%s','%s',current_timestamp() as dtm_created from invalid_addr_non_standardized_output"%(gold_database,gold_invalid_tbl,segment_name,advertiser_name,file_name))
   except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in writing in invalid_addr_uid_ip_addr_map table having segment_name as %s and advertiser_name as %s  for file_name:%s "%(segment_name,advertiser_name,file_name))
        raise e
        sys.exit(1)

   out_df1.createOrReplaceTempView("standardize_out")
   print("starting the join with standard address master table")
   try:
       std_addr_df = spark.sql("select standard_delivery_line_1,standard_delivery_line_2,standard_zipcode,standard_city,standard_state,enc_uhid  from gold_ntelligis.standard_address_master").repartition(200,"standard_delivery_line_1")
       std_addr_df.createOrReplaceTempView("standard_address_master")
       standardize_df=spark.sql("select  COALESCE(edpdecrypt('APUID','UID',B.enc_uhid,'%s','%s'),'-1') as uhid,A.src_street,A.src_zipcode,A.src_city,A.src_state from  standardize_out A LEFT OUTER  JOIN (select standard_delivery_line_1,standard_delivery_line_2,standard_zipcode,standard_city,standard_state,enc_uhid  from standard_address_master) B  on COALESCE(trim(A.std_delivery_line_1),'')=COALESCE(trim(B.standard_delivery_line_1),'') and COALESCE(trim(A.std_delivery_line_2),'')=COALESCE(trim(B.standard_delivery_line_2),'') and COALESCE(trim(A.std_zipcode),'')=COALESCE(trim(B.standard_zipcode),'') and COALESCE(trim(A.std_city),'')=COALESCE(trim(B.standard_city),'') and COALESCE(trim(A.std_state),'')=COALESCE(trim(B.standard_state),'')"%(decryption_user,dec_nn_service))

   except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in joining in incoming and gold stanadrad_address_master for getting uhid having segment_name as %s and advertiser_name as %s  for file_name:%s "%(segment_name,advertiser_name,file_name))
        raise e
        sys.exit(1)
		
		
   standardize_df.persist().createOrReplaceTempView("decrypted_uhid1")
   spark.sql("select distinct uhid from decrypted_uhid1 where uhid!='-1'").createOrReplaceTempView("decrypted_uhid")
   print("Writing to the invalid address table for non matched standard address records")
   try: 
        spark.sql("insert into table %s.%s   select src_street,src_zipcode,src_city,src_state,'NON_MATCHED_ADDRESS','%s','%s','%s',current_timestamp() as dtm_created from decrypted_uhid1 where uhid='-1'"%(gold_database,gold_invalid_tbl,segment_name,advertiser_name,file_name))
   except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in writing in invalid_addr_uid_ip_addr_map table having segment_name as %s and advertiser_name as %s  for file_name:%s "%(segment_name,advertiser_name,file_name))
        raise e
        sys.exit(1)
		
elif num_col==0:
   print("file has only one non empty column that is UHID columns, so no standardizing process required")
   spark.sql("select lower(trim(address)) as uhid from incoming_ntelligis.custom_onboarding_ip_matching where segment_name='%s' and advertiser_name='%s'"%(segment_name,advertiser_name)).distinct().cache().createOrReplaceTempView("decrypted_uhid")
else:
    print("invalid number of fields in the file for file:%s"%file_name)
    sys.exit(1)
#

try:
     df1=spark.sql("select nu_uhid,uhid from gold_ntelligis.uhid_neustar_map_master")
     df1.repartition(100,'uhid').createOrReplaceTempView("uhid_neustar_map_master")
     neu_uhid_DF=spark.sql("select COALESCE(B.nu_uhid,0) as nu_uhid,A.uhid from  decrypted_uhid A LEFT OUTER  JOIN  uhid_neustar_map_master B on A.uhid=B.uhid")
except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in joining in incoming decrypted_uhid and gold neustar_uhid for getting nu_uhid having segment_name as %s and advertiser_name as %s  for file_name:%s "%(segment_name,advertiser_name,file_name))
        raise e
        sys.exit(1)
neu_uhid_DF.createOrReplaceTempView("neustar_uhid")

try:
    df5=spark.sql("select * from gold_ntelligis.neustar_inward_extract")
    df5.filter("dsp_flag='Y'").select("custid").distinct().repartition(100,'custid').createOrReplaceTempView("neustar_inward_extract")
    cus_DF=spark.sql("select  COALESCE(B.custid,0) as nu_uhid,uhid from  neustar_uhid A LEFT OUTER  JOIN neustar_inward_extract  B on A.nu_uhid=B.custid")
    cus_DF.createOrReplaceTempView("neustar_uhid_dsp_yes")
except Exception as e:
            print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
            print ("error in joining in gold neustar_uhid  and gold neustart_inward_extract for nu_uhid for dsp_flag='Y'")
            raise e
            sys.exit(1)

try:
     spark.sql("select cuhid,uhid from gold_ntelligis.uhid_ap_map_master").repartition(100,'uhid').createOrReplaceTempView("uhid_ap_map_master")
     df2=spark.sql("select a.nu_uhid,a.uhid,COALESCE(b.cuhid,0) as cuhid from neustar_uhid_dsp_yes  a left outer join uhid_ap_map_master b on a.uhid=b.uhid").createOrReplaceTempView("uhid_nuuhid_cuhid_master")
except Exception as e:
      print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
      print ("error in joining with uhid_ap_map_master for getting cuhid for getting ip_addr for file_name:%s "%file_name)
      raise e
      sys.exit(1)
try:     
     df3=spark.sql("select * from gold_ntelligis.uhid_isp_map_master")
     df3.repartition(100,'cuhid').createOrReplaceTempView("uhid_isp_map_master")
     final_DF=spark.sql("select a.nu_uhid,a.uhid,COALESCE(b.ip_addr,0) as ip_addr from uhid_nuuhid_cuhid_master a left outer join (select cuhid,ip_addr from(select cuhid,ip_addr,row_number() over (partition by cuhid order by dtm_last_modified desc) as rnum from uhid_isp_map_master) tmp where tmp.rnum=1) b on a.cuhid=b.cuhid ")
except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in joining with uhid_isp_map_master for file_name:%s "%file_name)
        raise e
        sys.exit(1)

print(segment_name)
print("checking whether this segment_name as %s and advertiser_name as %s partition is already there or not for file as %s"%(segment_name,advertiser_name,file_name))
check_prev_segment_name_df=spark.sql("select * from  gold_ntelligis.custom_onboarding_ip_matching where segment_name='%s' and advertiser_name='%s' limit 100"%(segment_name,advertiser_name))
if len(check_prev_segment_name_df.take(1))==0:
   print("NO previous presence of this segment_name as %s and advertiser_name as %s"%(segment_name,advertiser_name))
   print("getting last max segment_id from the gold table")
   max_segment_id_df=spark.sql("select max(segment_id) from gold_ntelligis.custom_onboarding_ip_matching")
   segment_id=max_segment_id_df.select(max_segment_id_df[0]).first()[0]
   if segment_id is None:
      print("first time ingestion is taking place")
      segment_id=1
   else:
      segment_id+=1
else:
    print("this %s segment_name and %s advertiser_name  file %s had already been processed, so getting the existing segment_id for this file"%(segment_name,advertiser_name,file_name))
    segment_id=check_prev_segment_name_df.select(check_prev_segment_name_df[0]).first()[0]

print("writing into the table for file:%s"%file_name)
final_DF.withColumn("segment_id",lit("%d"%segment_id)).repartition(20,'uhid').createOrReplaceTempView("uhid_ip_addr_map_output")

try:
        spark.sql("insert overwrite  table %s.%s  partition(segment_name='%s',advertiser_name='%s') select segment_id,uhid,nu_uhid,ip_addr,'%s',current_timestamp() as dtm_created from uhid_ip_addr_map_output"%(gold_database,gold_table,segment_name,advertiser_name,file_name))
except Exception as e:
        print (str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        print ("error in writing in gold uhid ip mapping map  table for file_name:%s "%file_name)
        raise e
        sys.exit(1)

print("successfully table created")
