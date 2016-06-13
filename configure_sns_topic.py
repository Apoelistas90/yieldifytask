import boto3
import sys
import json

# https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html
# Step 1: Create an Amazon SNS Topic - DONE IN AWS CONSOLE (right click edit policy)
# Step 2: Create an Amazon SQS Queue
# Step 3: Add a Notification Configuration to Your Bucket - DONE IN AWS CONSOLE
sns = boto3.client('sns')


s3bucket = 'yieldifyadamides'
s3_key_prefix = 'source/'

s3_full_path = s3bucket + '/' + s3_key_prefix
print(s3_full_path)


topic_attribute_input =  sys.argv[1]
topic_attribute_json = None
with open(topic_attribute_input) as json_file:
    topic_attribute_json = json.load(json_file)

sns_topic_arn = sns.create_topic(Name = 'yieldifytask')
topic_attribute_json['Statement'][0]['Resource'] = str(sns_topic_arn['TopicArn'])
arn = str(sns_topic_arn['TopicArn'])
sources3arn = 'arn:aws:s3:*:*:' + s3_full_path
topic_attribute_json['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = 'arn:aws:s3:*:*:' + s3_full_path



attr_val2 = {
 'Version': '2008-10-17',
 'Id': 'example-ID',
 'Statement': [
  {
   'Sid': 'example-statement-ID',
   'Effect': 'Allow',
   'Principal': {
    'AWS':'*'
   },
   'Action': [
    'SNS:Publish'
   ],
   'Resource': 'arn:aws:sns:eu-west-1:957284751767:yieldifytask',
   'Condition': {
      'ArnLike': {
      'aws:SourceArn': 'arn:aws:s3:*:*:yieldifyadamides'
    }
   }
  }
 ]
}

result = sns.set_topic_attributes(TopicArn = arn,
                         AttributeName = 'Policy',
                         AttributeValue=str(attr_val2))
print(result)