'''
This is a script which will filter out a set of users from all
tracking logs into a new bucket.

This script consists of two pieces: 

1) Send all the tracking logs to an SQS queue
2) Process all the tracking logs from the SQS queue

As a ballpark, this script ought to take about a day of machine time
on a fast machine per terabyte of tracking logs (estimated, not from
actual performance). If it's off by an order of magnitude, either I'm
doing something wrong here or elsewhere.
'''


import uuid
import gzip
import os
import boto.sqs
import xanalytics.settings
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
temp = uuid.uuid1().hex

users=[x.strip() for x in open("/tmp/cluster/cluster/forum_run/user_list.txt")]

def process(item):
    outfile = uuid.uuid1().hex+".gz"
    dest = gzip.open(outfile, "w")
    source_bucket = s3_conn.get_bucket(xanalytics.settings.settings['tracking-logs-bucket')
    key = source_bucket.get_key(item)
    filename = "/tmp/log_"+uuid.uuid1().hex+".log"
    key.get_contents_to_filename(filename)
    try:
        lines = gzip.open(filename).readlines()
    except IOError:
        lines = open(filename).readlines()
    for line in lines: 
        good = False
        for user in users:
            if user in line:
                good = True
        if good:
            dest.write(line)
    dest.close()
    outbucket = s3_conn.get_bucket(xanalytics.settings.settings['scratch_bucket'])
    k = outbucket.new_key("forums/"+item)
    k.set_contents_from_filename(outfile)
    os.unlink(outfile)
    os.unlink(filename)

i = 0

import boto.sqs
sqs_conn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
q = sqs_conn.get_queue(xanalytics.settings.settings["tracking-logs-queue"])
while q.count() > 0:
    m = q.read(60*5)
    b = m.get_body()
    i = i+1
    print i, b
    process(b)
    q.delete_message(m)

