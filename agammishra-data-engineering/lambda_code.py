# Set up logging
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')
s3 = boto3.client('s3')

# Variables for the job: 
glueJobName = "covid_ma_job"

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## TRIGGERED BY EVENT: ')
    
    file_obj=event['Records'][0]
    file_name=str(file_obj['s3']['object']['key'])
    bucket_name=str(file_obj['s3']['bucket']['name'])
    file_to_process='s3://'+bucket_name+'/'+file_name
    partition_value=file_name.split('.')[0]
    print('partition_value: ',partition_value)
    print('FILE TO BE PROCESSED: ',file_to_process)
    print('triggering glue job now')
    response = client.start_job_run(JobName = glueJobName,Arguments = {
                '--new_file_name': file_to_process,
                '--partition_value': partition_value})
             
    logger.info('## STARTED GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response
