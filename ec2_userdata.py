#!/usr/bin/python27

import boto3
import json
import ast
import time
import os
import tempfile
import shutil
import constants

# define constants
SQS_QUEUE_NAME = constants.SQS_QUEUE_NAME
SQS_VISIBILITY_TIMEOUT = constants.SQS_VISIBILITY_TIMEOUT
SQS_WAIT_TIME_SECONDS = constants.SQS_WAIT_TIME_SECONDS
SQS_MESSAGE_VALIDATION_STRING = constants.SQS_MESSAGE_VALIDATION_STRING

S3_PROFILE_NAME = constants.S3_PROFILE_NAME
TEMP_DIR = constants.TEMP_DIR
SLEEP_SECONDS = constants.SLEEP_SECONDS

# setting up connection with S3
session = boto3.session.Session(profile_name=S3_PROFILE_NAME)
s3 = session.resource('s3')
# setting up connection with SQS
sqs = boto3.client('sqs')
queue = sqs.get_queue_url(QueueName = SQS_QUEUE_NAME)


while(True):
    # Get a message
    # https://aws.amazon.com/blogs/aws/amazon-sqs-long-polling-batching/
    #VisibilityTimeout = times*3 normal time to be safe

    # attempt to see if new message is available
    message = sqs.receive_message( QueueUrl=queue['QueueUrl'], VisibilityTimeout=SQS_VISIBILITY_TIMEOUT, WaitTimeSeconds=SQS_VISIBILITY_TIMEOUT)

    # if 'Messages' exist in the received message then it means we have a received a new message
    if message is not None and SQS_MESSAGE_VALIDATION_STRING in message:
        receipt_handle = message['Messages'][0]['ReceiptHandle']

        lower_message_body = ast.literal_eval(message['Messages'][0]['Body'])

        awsregion = ast.literal_eval(lower_message_body['Message'])['Records'][0]['awsRegion']
        bucket = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['bucket']['name']
        object_key = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['object']['key']

        print receipt_handle
        print awsregion
        print bucket
        print object_key

        #can extend visibility time VisibilityTimeout

        # Download file from S3

        tempdir = tempfile.mkdtemp(prefix=TEMP_DIR)
        # first, get the client on which our s3 resource is based
        s3c = s3.meta.client
        # set key and file name
        s3_object_name = object_key
        file_name = os.path.basename(s3_object_name)
        file_path = os.path.join(tempdir, file_name)

        #print('Downloading ' + s3_object_name + ' to ' + file_path)
        s3c.download_file(bucket, s3_object_name, file_path)

        # main ETL process here

        #*****review
        sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
        shutil.rmtree(tempdir)
    else:
        print "No message!"

    #time.sleep(SLEEP_SECONDS)


