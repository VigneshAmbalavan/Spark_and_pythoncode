# Set up logging
import json
import os
import logging
import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')
s3 = boto3.client('s3')

# Variables for the job: 
glueJobName = "covid_ma_job_new"

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## TRIGGERED BY EVENT: ')
    
    file_obj=event['Records'][0]
    file_name=str(file_obj['s3']['object']['key'])
    bucket_name=str(file_obj['s3']['bucket']['name'])
    upper_bound_date1=file_name.split('/')[1].split('.')[0]
    upper_bound_date=datetime.datetime.strptime(upper_bound_date1, '%m-%d-%Y').date()+datetime.timedelta(days=1)
    lower_bound_date=(upper_bound_date - datetime.timedelta(days=7))
    print('lower: ',lower_bound_date.strftime("%Y-%m-%d"))
    print('upper: ',upper_bound_date.strftime("%Y-%m-%d"))
    logger.info('## checking the glue job trigger eligibility')
    num_files_at_s3=glue_job_trigger_eligibility(bucket_name)
    if num_files_at_s3==7:
        logger.info('triggering the glue job to calculate the MA')
        res=glue_job_trigger(lower_bound_date,upper_bound_date)
        return res
    else:
        logger.info('cant trigger the job as still no at least 7 days file')
    
    
    
def glue_job_trigger_eligibility(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    print(response['Contents'])
    count=0
    for object in response['Contents']:
      if object['Size']!= 0:
          count+=1
          logger.info(object['Key'])
    return count
    
def glue_job_trigger(lower_bound_date,upper_bound_date):
      
        logger.info('triggering glue job now')
        response = client.start_job_run(JobName = glueJobName,Arguments = {
                        '--lower_bound_date': lower_bound_date,
                        '--upper_bound_date': upper_bound_date })
                 
        logger.info('## STARTED GLUE JOB: ' + glueJobName)
        logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
        return response
        
    
