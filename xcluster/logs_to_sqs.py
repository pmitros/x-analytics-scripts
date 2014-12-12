'''
Take a list of all edX tracking logs in an S3 bucket. 

Dump that list to Amazon SQS for worker processes to pull from.
'''

import boto.sqs
import xanalytics.settings
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
bucket = s3_conn.get_bucket(xanalytics.settings.settings["tracking-logs-bucket"])

sqs_conn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
q = sqs_conn.get_queue(xanalytics.settings.settings["tracking-logs-queue"])

for key in bucket.list():
    if not key.name.startswith("logs/tracking"):
        continue

    m = Message()
    m.set_body(key.name.encode('utf-8'))
    q.write(m)
    print m
