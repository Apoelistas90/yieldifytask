# https://www.pypa.io/en/latest/
# https://alestic.com/2014/12/s3-bucket-notification-to-sqssns-on-object-creation/
# http://www.slideshare.net/AmazonWebServices/massive-message-processing-with-amazon-sqs-and-amazon-dynamodb-arc301-aws-reinvent-2013-28431182

import sys
import csv
import gzip as gz
import json
#from utils import *
import utils
import constants
import boto3
import tempfile
import os
import shutil
import time

# Need logging for each of the methods. If at least one has failed then file not processed

# http://www.slideshare.net/AmazonWebServices/massive-message-processing-with-amazon-sqs-and-amazon-dynamodb-arc301-aws-reinvent-2013-28431182
def parse_and_transform_file(input_file):
    # function variables
    tranformed_data = {}
    tranformed_record = {}
    counter=1
    # decompress the file and process according to wanted output
    with gz.open(input_file, 'rb') as tsvfile:
        records = csv.reader(tsvfile, delimiter='\t')
        # get indices
        date = constants.DATE_LOCATION
        time = constants.TIME_LOCATION
        url = constants.URL_LOCATION
        ip = constants.IP_LOCATION
        ua = constants.UA_LOCATION

        # go through each record and
        # 1. store relevant information in a json(Done)
        # 2. insert data in DynamoDB for later querying through API service(Not yet started)
        for record in records:
            if(len(record) == constants.TOTAL_LENGTH):
                if(utils.validate_date(record[date]) and utils.validate_time(record[time])):
                    tranformed_record['timestamp'] = record[date] + ' ' + record[time]
                if(utils.validate_url(record[url])):
                    tranformed_record['url'] = record[url]
                if(utils.validate_ip(record[ip])):
                    tranformed_record['location'] = utils.process_geolocation_data(record[ip])
                if(record[ua] != ''): #need to test this
                    tranformed_record['user_agent'] = utils.process_user_agent(record[ua])
                tranformed_data[counter] = tranformed_record
                counter+=1

    # convert directly dict to json and return
    return json.dumps(tranformed_data)


def upload_to_s3(output_json,source_file_name,s3c,s3_bucket,s3_key_prefix):
    # creating temporary directory for our file
    tempdir = tempfile.mkdtemp(prefix='yieldify')
    file_name = os.path.basename('tranformed_' + source_file_name )
    file_path = os.path.join(tempdir, file_name)
    result = False

    print(output_json)

    with gz.open(file_path, 'wb') as gzfile:
        gzfile.write(output_json)
        print('Uploading ' + file_path + ' to ' + s3_key_prefix + file_name)

        # delete temporary files and directory from local disk
        gzfile.close()

    # Deal with result
    result = s3c.upload_file(file_path, s3_bucket, s3_key_prefix + file_name)
    print(result)
    shutil.rmtree(tempdir)
    return result





