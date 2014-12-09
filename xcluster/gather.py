'''
Download all processed tracking logs to local machine
'''

import boto.sqs
import xanalytics.settings
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
bucket = s3_conn.get_bucket(xanalytics.settings.settings["scratch-bucket"])

l = list(bucket.list())
l = [i for i in l if i.name.startswith("forums/logs")]
print "Total size", sum(i.size for i in l)

i=0
for key in l:
     key.get_contents_to_filename(str(i))
     i = i+1
