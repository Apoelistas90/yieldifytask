#!/usr/bin/python27

import boto3
import ast
import time
import os
import tempfile
import shutil
import constants
import etl_functions

#todo = log complete files in rds or dynamo
#todo = test service
#todo = review user agent processing
#todo = check if file exists in s3 inbox and if yes move to working s3 and queue



# Setup SQS connection
sqs = boto3.client('sqs',region_name='eu-west-1')
queue = sqs.get_queue_url(QueueName = constants.SQS_QUEUE_NAME)
# Setup S3 connection
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def start():

    # Main routine
    while(True):
        # Get a message
        # https://aws.amazon.com/blogs/aws/amazon-sqs-long-polling-batching/
        #VisibilityTimeout = times*3 normal time to be safe

        # get number of files in inbox directory. if zero sleep and continue
        # if more than 0 move them one by one in working directory which will trigger a message in sqs
        result = s3_client.list_objects(Bucket=constants.S3_DESTINATION_BUCKET,
                                        Prefix=constants.S3_SOURCE_DIRECTORY,
                                        Delimiter='|')
        exit(0)



        # attempt to see if new message is available
        message = sqs.receive_message( QueueUrl=queue['QueueUrl'],
                                       VisibilityTimeout=constants.SQS_VISIBILITY_TIMEOUT,
                                       WaitTimeSeconds=constants.SQS_VISIBILITY_TIMEOUT)

        # if 'Messages' key exists in the received message then it means the json we received contains a message,
        # otherwise it contains just the request id only
        if message is not None and constants.SQS_MESSAGE_VALIDATION_STRING in message:
            # Get details from message
            receipt_handle = message['Messages'][0]['ReceiptHandle']
            lower_message_body = ast.literal_eval(message['Messages'][0]['Body'])
            bucket = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['bucket']['name']
            object_key = ast.literal_eval(lower_message_body['Message'])['Records'][0]['s3']['object']['key']

            #can extend visibility time while in the loop by putting timers

            # Download file from S3 in a temp directory for processing
            tempdir = tempfile.mkdtemp(prefix=constants.TEMP_DIR)
            source_file_name = os.path.basename(object_key)
            file_path = os.path.join(tempdir, source_file_name)
            print('Downloading ' + object_key + ' to ' + file_path)
            s3.meta.client.download_file(bucket, object_key, file_path)

            # Parse and process input file
            print('Processing ' + object_key + '.....')
            output_json = etl_functions.parse_and_transform_file(file_path)
            print('Processing complete!')
            # Compress and upload output file in S3
            etl_functions.upload_to_s3(output_json,source_file_name,s3,
                                       constants.S3_DESTINATION_BUCKET,constants.S3_DESTINATION_PREFIX)

            #*****review when to delete
            sqs.delete_message(QueueUrl=queue['QueueUrl'], ReceiptHandle=receipt_handle)
            shutil.rmtree(tempdir)

            # Log that this file has been processed successfully including timestamp of completion


        else:
            print "No message!"

        time.sleep(constants.SLEEP_SECONDS)

if __name__ == "__main__":
    start()


