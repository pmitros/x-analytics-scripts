'''
Download all processed tracking logs to local machine
'''

import md5
import os
import os.path

import boto.sqs
from xanalytics.settings import settings
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

if __name__ == '__main__':
    s3_conn = S3Connection(
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    bucket = s3_conn.get_bucket(settings["scratch-bucket"])

    l = list(bucket.list())
    l = [i for i in l if i.name.startswith("forums/logs")]
    print "Total size", sum(i.size for i in l)

    i = 0
    for key in l:
        filename = md5.new()
        filename.update(key.name)
        filename = "out/"+filename.hexdigest()
        if not os.path.exists(filename):
            key.get_contents_to_filename(filename)
        i = i+1
        print i
