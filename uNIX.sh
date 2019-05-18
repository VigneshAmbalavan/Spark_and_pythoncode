#!/bin/bash

pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null



source ${SCRIPT_HOME}/../config/custom_onboarding_ip_matching_bash.properties
export SPARK_MAJOR_VERSION=2




household_ids_jar=`ls ${SCRIPT_HOME}/../lib/householdids-*.jar`
smartystreets_jar=`ls ${SCRIPT_HOME}/../lib/smartystreets-java-sdk-*.jar`
aud_partners_jar=`ls ${SCRIPT_HOME}/../lib/edp-audiencepartners-*.jar`
other_jars=`echo ${SCRIPT_HOME}/../lib/google-http-client-*.jar | tr ' ' ','`
pg_jar=$(echo $common_lib_path/postgresql-9*.jar | tr ' ' ',')
hive_jar=`ls ${SCRIPT_HOME}/../lib/edp-hive-udf-*.jar`
haship_jar=$common_lib_path/edp-ap-compiler-1.1.0-SNAPSHOT.jar

hive_site_xml=/usr/hdp/current/spark2-client/conf/hive-site.xml
py_config_file=${SCRIPT_HOME}/../config/pg.ini
config_file=${SCRIPT_HOME}/../config/custom_onboarding_ip_matching_bash.properties


ts=`date +"%Y%m%d"`
time_stamp=`date +"%Y%m%d_%H%M%S"`
log_file=${ts}.log



echo "Creating log dir if not exists : $log_file_path"
mkdir -p $log_file_path
final_log_path=$log_file_path/custom_onboarding_ip_matching_load_extract_tos3_${log_file}

if [ ! -f $final_log_path ]; then
    echo "File not found,so creating new for ${ts} date" | tee -a ${final_log_path}
else
    printf "\n---------------------------------------------log file for $ts date is already present,so addig the new run for timestamp as ${time_stamp}------------------------\n\n\n" | tee -a ${final_log_path}
fi

hadoop fs -rm -r -f ${output_extract_hdfs}/*
file_name=$1

if [ ! -z $file_name ]
   then
        echo "starting the process of loading to gold  table for file: "${file_name}"" | tee -a ${final_log_path}

   else
        echo "no value of file and column count is being passed for file:"${file_name}""| tee -a ${final_log_path}
        exit 1
fi
echo "getting the latest partition value for incoming_ntelligis.segment_ip_mapping"
latest_date_partition=$(beeline  -u ${hs2_url} --showHeader="false" --outputformat="tsv2" -e "select max(source_date) from incoming_ntelligis.segment_ip_mapping")
if [ $? -eq 0 ]
then
     echo "sucessfully got the latest ip_segment_mapping latest available partition as $latest_date_partition" | tee -a ${final_log_path}
else
     echo "not able to get the latest ip_segment_mapping latest available partition" | tee -a ${final_log_path}
     exit 1
fi

echo "Loading extracts to s3  for files : ${file_name}" | tee -a ${final_log_path}
spark-submit --master yarn --files ${hive_site_xml},${py_config_file},${config_file}  --deploy-mode client --conf spark.sql.autoBroadcastJoinThreshold=-1 --jars ${aud_partners_jar},${smartystreets_jar},${other_jars},${pg_jar},${hive_jar},${haship_jar} --num-executors ${num_executors} --executor-cores ${executor_cores} --driver-memory ${driver_memory}  --executor-memory ${executor_memory}  --driver-class-path '*' ${SCRIPT_HOME}/custom_onboarding_ip_matching_load_extract_tos3.py "${file_name}" "${latest_date_partition}" "${py_config_file}" &>> ${final_log_path}
 
if [ $? -ne 0 ]
then
     echo "Failed in copying extracts"  | tee -a ${final_log_path}
     exit 1
else
     echo "Successfull in copying extracts"  | tee -a ${final_log_path}
fi


end_ts=`date +"%Y%m%d_%H%M%S"`  
echo "Start time: $time_stamp  End time $end_ts"  | tee -a ${final_log_path}
