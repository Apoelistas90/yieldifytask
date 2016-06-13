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

# Need logging for each of the methods. If at least one has failed then file not processed

# http://www.slideshare.net/AmazonWebServices/massive-message-processing-with-amazon-sqs-and-amazon-dynamodb-arc301-aws-reinvent-2013-28431182
def download_file():
    # Give me the list of S3 buckets in the AWS account
    s3_resource = boto3.resource('s3')
    print("List of buckets:")
    buckets = s3_resource.buckets.all()
    for bucket in buckets:
        print(bucket.name)
    print("End of listing buckets")
    print('\n')


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
    print(tranformed_data)

    # convert directly dict to json and return
    return json.dumps(tranformed_data)


def upload_to_s3(transformed_file):
    # creating temporary directory for our file
    tempdir = tempfile.mkdtemp(prefix='yieldify')
    file_name = os.path.basename('sample_data_tranformed.gz')
    file_path = os.path.join(tempdir, file_name)

    with gz.open(file_path, 'wb') as gzfile:
        gzfile.write(output_json)
        # setting up connection with S3
        profile_name = "default"
        session = boto3.session.Session(profile_name=profile_name)
        s3 = session.resource('s3')
        s3c = s3.meta.client
        s3_key_prefix = 'uploads/'
        s3_key = s3_key_prefix + file_name
        # logging
        print('Uploading ' + file_path + ' to ' + s3_key)
        s3c.upload_file(file_path, 'yieldifyadamides', s3_key)
        # delete temporary files and directory from local disk
        shutil.rmtree(tempdir)
    return True

if __name__ == "__main__":
    #download file
    #download_file()
    sample_file = sys.argv[1]

    # parse and process input file
    output_json = parse_and_transform_file(sample_file)
    # compress and upload output file
    result = upload_to_s3(output_json)

