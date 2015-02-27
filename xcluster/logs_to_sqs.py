'''
Take a list of all edX tracking logs in an S3 bucket. 

Dump that list to Amazon SQS for worker processes to pull from.

Example:

  python logs_to_sqs.py --bucket edx-analytics-source --prefix user-history/ --queue edx-sqs-worker-queue
'''

import boto.sqs
import xanalytics.settings
import xanalytics.multiprocess
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

import sys, os
import argparse

parser = argparse.ArgumentParser(description='Send a set of files to SQS.')
parser.add_argument('--bucket', dest='bucket', 
                    default = xanalytics.settings.settings["tracking-logs-bucket"], help='Source bucket')
parser.add_argument('--prefix', dest='prefix', 
                    default = "logs/tracking", help='Bucket prefix')
parser.add_argument('--queue', dest='queue', 
                    default = xanalytics.settings.settings["tracking-logs-queue"], help='SWS queue')
args = parser.parse_args()

s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], 
                       aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
bucket = s3_conn.get_bucket(args.bucket)

sqs_conn = boto.sqs.connect_to_region("us-east-1", 
                                      aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], 
                                      aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])

source = bucket.list(prefix=args.prefix)
source = xanalytics.multiprocess.split(source, 100)

q = sqs_conn.get_queue(args.queue)

r = []

for key in source:
    r.append(None)
    if not key.name.startswith(args.prefix):
        continue

    m = Message()
    m.set_body(key.name.encode('utf-8'))
    q.write(m)

xanalytics.multiprocess.join(r)
