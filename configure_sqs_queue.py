# https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html
# Step 2: Create an Amazon SQS Queue


import boto3

sqs = boto3.client('sqs')

# Create queue
result = sqs.create_queue(QueueName='myqueue')
print(result)

# Give the SNS topic permission to post to the SQS queue

    # Updated in AWS console

# Subscribe SQS queue to SNS topic

    # Updated in AWS console
