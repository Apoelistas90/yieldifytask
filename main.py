import sys
import csv
import gzip as gz
import json
import utils,constants
# Need logging for each of the methods. If at least one has failed then file not processed

def download_file():
    return

def parse_and_transform_file(input_file):
    #function variables
    tranformed_data = {}
    tranformed_record = {}
    counter=1
    #decompress the file and process according to wanted output
    with gz.open(input_file, 'rb') as tsvfile:
        records = csv.reader(tsvfile, delimiter='\t')

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

    #convert directly dict to json and return
    return json.dumps(tranformed_data)


def upload_to_s3(transformed_file):

    #upload to S3

    return

if __name__ == "__main__":
    #download file
    sample_file = sys.argv[1]

    #parse and process input file
    output_json = parse_and_transform_file(sample_file)
    #compress output file
    exit(0)
    with gz.open('sample_data_tranformed.gz', 'wb') as gzfile:
        gzfile.write(output_json)
    #upload output file