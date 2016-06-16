#!/usr/bin/python27

import boto3
import ast
import time
import os
import tempfile
import shutil
import constants
import etl_functions
from boto3.dynamodb.conditions import Key, Attr


# Setup SQS connection
sqs = boto3.client('sqs',region_name='eu-west-1')
queue = sqs.get_queue_url(QueueName = constants.SQS_QUEUE_NAME)
# Setup S3 connection
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
# Setup DynamoDB
dynamodb = boto3.resource('dynamodb')
filenames_table = dynamodb.Table(constants.DYNAMO_FILES_TABLE)


def routine_etl(object_key):

    # Download file in temp for processing
    tempdir = tempfile.mkdtemp(prefix=constants.TEMP_DIR)
    source_file_name = os.path.basename(object_key)
    file_path = os.path.join(tempdir, source_file_name)

    print('Downloading ' + object_key + ' to ' + file_path)
    s3.meta.client.download_file(constants.S3_DESTINATION_BUCKET,
                                 object_key,
                                 file_path)

    if not os.path.isfile(file_path):
        return False,'Download failed'

    # Parse and process input file
    print('Processing ' + object_key + '.....')
    output_json = etl_functions.parse_and_transform_file(file_path)
    print('Processing complete!')

    # Compress and upload output file in S3
    status, msg = etl_functions.upload_to_s3(output_json,
                                             source_file_name,
                                             s3,
                                             constants.S3_DESTINATION_BUCKET,
                                             constants.S3_DESTINATION_PREFIX)
    shutil.rmtree(tempdir)

    return status,msg


def process_current_files():
    result = s3_client.list_objects(Bucket=constants.S3_DESTINATION_BUCKET,
                                    Prefix=constants.S3_SOURCE_DIRECTORY,
                                    Delimiter='|')
    total_files = len(result['Contents'])

    # If count is one then this is the dir itself as is treated as a key
    if total_files == 1:
        return True, 'No files to process'
    elif total_files > 1:
        # Loop through files
        for index in range(0,total_files):
            if '.gz' not in result['Contents'][index]['Key']:
                continue
            object_key = result['Contents'][index]['Key']
            # if this file has been processed before continue to next file
            filename = object_key.split(constants.S3_SOURCE_DIRECTORY)[1]
            res = filenames_table.query(KeyConditionExpression=Key(constants.DYNAMO_FILES_TABLE_PK).eq(filename))
            if res['Count'] == 1:
                print('File ' + filename + ' already processed.. Skipping this.')
                continue
            # otherwise process it
            else:
                complete, msg = routine_etl(object_key)
                if not complete:
                    return complete, msg
                # add filename to dynamo table
                filenames_table.put_item(Item = {constants.DYNAMO_FILES_TABLE_PK: filename})

        return True, 'Processing of existing files complete!'
    else:
        return False, 'Incorrect prefix, directory does not exist...'


def start():

    # Main routine
    while True:
        # attempt to see if new message is available in the SQS queue
        sqs_message = sqs.receive_message( QueueUrl=queue['QueueUrl'],
                                           VisibilityTimeout=constants.SQS_VISIBILITY_TIMEOUT,
                                           WaitTimeSeconds=constants.SQS_VISIBILITY_TIMEOUT)

        """ Validate API response from SQS to see if it has any messages to process
         Validation process: If the message contains constants.SQS_MESSAGE_VALIDATION_STRING('Messages')
         as one of its keys then this means there are messages to process"""
        if sqs_message is not None and constants.SQS_MESSAGE_VALIDATION_STRING in sqs_message:
            # Get message details
            receipt_handle = sqs_message['Messages'][0]['ReceiptHandle']
            lower_message_body = ast.literal_eval(sqs_message['Messages'][0]['Body'])

            # Check if this is a test event message, happens when firstly creating event notification
            if 'Event' in ast.literal_eval(lower_message_body['Message']):
                sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
                continue

            # Main ETL process here
            object_key = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['object']['key']
            filename = object_key.split(constants.S3_SOURCE_DIRECTORY)[1]
            res = filenames_table.query(KeyConditionExpression=Key(constants.DYNAMO_FILES_TABLE_PK).eq(filename))
            # If file has been already processed delete sqs message and continue
            if res['Count'] == 1:
                print('File '+ filename + ' already processed.. Skipping this.')
                sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
            # Else process it and then delete message
            else:
                status, completion_msg = routine_etl(object_key)

                if status:
                    # Processing is complete, delete message from queue
                    sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
                    # add filename to dynamo table
                    filenames_table.put_item(Item = {constants.DYNAMO_FILES_TABLE_PK: filename})
                else:
                    # There was an issue with current message, alert here is desirable
                    print(completion_msg)
        else:
            print "No message!"

        time.sleep(constants.SLEEP_SECONDS)

if __name__ == "__main__":
    done, msg = process_current_files()
    if done:
        print(msg)
        start()
    else:
        print('There was an issue in processing the current files in the bucket, please investigate:')
        print(msg)



