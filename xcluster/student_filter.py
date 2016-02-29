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
from xanalytics.settings import settings
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

s3_conn = S3Connection(
    aws_access_key_id=settings['edx-aws-access-key-id'],
    aws_secret_access_key=settings['edx-aws-secret-key']
)
temp = uuid.uuid1().hex

users = [x.strip()
         for x
         in open("/tmp/cluster/cluster/forum_run/user_list.txt")]


def process(item):
    outfile = uuid.uuid1().hex+".gz"
    dest = gzip.open(outfile, "w")
    source_bucket = s3_conn.get_bucket(settings['tracking-logs-bucket'])
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
    outbucket = s3_conn.get_bucket(settings['scratch-bucket'])
    k = outbucket.new_key("forums/"+item)
    k.set_contents_from_filename(outfile)
    os.unlink(outfile)
    os.unlink(filename)
    return key.size

i = 0
d = 0

sqs_conn = boto.sqs.connect_to_region(
    "us-east-1",
    aws_access_key_id=settings['edx-aws-access-key-id'],
    aws_secret_access_key=settings['edx-aws-secret-key']
)
q = sqs_conn.get_queue(settings["tracking-logs-queue"])
while q.count() > 0:
    m = q.read(60*10)
    b = m.get_body()
    i = i+1
    print b
    s = process(b)
    d = d + s
    print i, "files", b, d/1.e9, "GB"
    q.delete_message(m)
