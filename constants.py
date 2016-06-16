# Indices in the input file
DATE_LOCATION=0
TIME_LOCATION=1
USER_ID_LOCATION=2
URL_LOCATION=3
IP_LOCATION=4
UA_LOCATION=5
TOTAL_LENGTH=6

# API details for IP API
GEOLOCATIONURL = 'http://api.ipinfodb.com/v3/ip-city/?key='
APIKEY = 'a24bab79e02ae0e4083b9327dc2c49a9f76babbac85486d99306fa0e18110f95'

# S3 details
S3_SOURCE_DIRECTORY = 'testdir/'
S3_WORKING_DIRECTORY = 'inbox/'
S3_DESTINATION_BUCKET = 'yieldifyadamides'
S3_DESTINATION_PREFIX = 'uploads/'
S3_PROFILE_NAME = "default"

# SQS details
SQS_QUEUE_NAME = 'myqueue'
SQS_VISIBILITY_TIMEOUT = 1         #VisibilityTimeout = times*3 normal time to be safe , # https://aws.amazon.com/blogs/aws/amazon-sqs-long-polling-batching/
SQS_WAIT_TIME_SECONDS = 1
SQS_MESSAGE_VALIDATION_STRING = 'Messages'
SLEEP_SECONDS = 2

# Other
TEMP_DIR = 'yieldify'





