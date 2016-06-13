#!/usr/bin/python27

import boto3
import ast
import time
import os
import tempfile
import shutil
import constants
import wrapper


# SQS related constants
SQS_QUEUE_NAME = constants.SQS_QUEUE_NAME
SQS_VISIBILITY_TIMEOUT = constants.SQS_VISIBILITY_TIMEOUT
SQS_WAIT_TIME_SECONDS = constants.SQS_WAIT_TIME_SECONDS
SQS_MESSAGE_VALIDATION_STRING = constants.SQS_MESSAGE_VALIDATION_STRING
# S3 related constants
S3_PROFILE_NAME = constants.S3_PROFILE_NAME
S3_DESTINATION_BUCKET = constants.S3_DESTINATION_BUCKET
S3_DESTINATION_PREFIX = constants.S3_DESTINATION_PREFIX
# Other constants
TEMP_DIR = constants.TEMP_DIR
SLEEP_SECONDS = constants.SLEEP_SECONDS

# Setup SQS connection
sqs = boto3.client('sqs')
queue = sqs.get_queue_url(QueueName = SQS_QUEUE_NAME)
# Setup S3 connection
session = boto3.session.Session(profile_name=S3_PROFILE_NAME)
s3 = session.resource('s3')
s3c = s3.meta.client

# Main routine
while(True):
    # Get a message
    # https://aws.amazon.com/blogs/aws/amazon-sqs-long-polling-batching/
    #VisibilityTimeout = times*3 normal time to be safe

    # attempt to see if new message is available
    message = sqs.receive_message( QueueUrl=queue['QueueUrl'],
                                   VisibilityTimeout=SQS_VISIBILITY_TIMEOUT,
                                   WaitTimeSeconds=SQS_VISIBILITY_TIMEOUT)

    # if 'Messages' key exists in the received message then it means the json we received contains a message,
    # otherwise it contains just the request id only
    if message is not None and SQS_MESSAGE_VALIDATION_STRING in message:
        # Get details from message
        receipt_handle = message['Messages'][0]['ReceiptHandle']
        lower_message_body = ast.literal_eval(message['Messages'][0]['Body'])
        bucket = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['bucket']['name']
        object_key = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['object']['key']

        #can extend visibility time while in the loop by putting timers

        # Download file from S3 in a temp directory for processing
        tempdir = tempfile.mkdtemp(prefix=TEMP_DIR)
        source_file_name = os.path.basename(object_key)
        file_path = os.path.join(tempdir, source_file_name)
        print('Downloading ' + object_key + ' to ' + file_path)
        s3c.download_file(bucket, object_key, file_path)

        # Parse and process input file
        print('Processing ' + object_key + '.....')
        output_json = wrapper.parse_and_transform_file(file_path)
        print('Processing complete!')
        # Compress and upload output file
        wrapper.upload_to_s3(output_json,source_file_name,s3c,S3_DESTINATION_BUCKET,S3_DESTINATION_PREFIX)

        #*****review when to delete
        sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
        shutil.rmtree(tempdir)
    else:
        print "No message!"

    time.sleep(SLEEP_SECONDS)


