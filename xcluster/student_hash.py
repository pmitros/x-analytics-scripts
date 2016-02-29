'''
The eventual goal is to have per-user sorted stack traces.
'''


import uuid
import gzip
import platform
import os
import os.path
import boto.sqs
from xanalytics.settings import settings
from xanalytics.streaming import *
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

s3_conn = S3Connection(
    aws_access_key_id=settings['edx-aws-access-key-id'],
    aws_secret_access_key=settings['edx-aws-secret-key']
)

filebase = "/mnt/log/"+str(os.getpid())+"-"+platform.node()
if not os.path.exists(filebase):
    os.makedirs(filebase)

files = {}


def sqs_lines():
    '''
    If we have a set of tracking log files on Amazon S3, this lets us
    grab all of the lines, and process them.

    In most cases, this script would be running in parallel on a
    cluster of machines. This lets us process many files quickly.

    logs_to_sqs is a good helper script for setting things up.
    '''
    import boto.sqs
    sqs_conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    q = sqs_conn.get_queue(settings["tracking-logs-queue"])
    file_count = 0
    total_bytes = 0
    while q.count() > 0:
        m = q.read(60*20)  # We limit processing to 20 minutes per file
        item = m.get_body()
        file_count = file_count+1
        print item

        source_bucket = s3_conn.get_bucket(settings['tracking-logs-bucket'])
        key = source_bucket.get_key(item)
        filename = "/mnt/tmp/log_"+uuid.uuid1().hex+".log"
        key.get_contents_to_filename(filename)
        try:
            lines = gzip.open(filename).readlines()
        except IOError:
            lines = open(filename).readlines()
        for line in lines:
            yield line

        total_bytes = total_bytes + key.size
        print file_count, "files", item, total_bytes/1.e9, "GB"
        q.delete_message(m)

        os.unlink(filename)


data = sqs_lines()
data = text_to_json(data)
data = remove_redundant_data(data)
data = truncate_json(data, 200)

files = {}

for item in data:
    user = item.get("username", "___NONE___")
    hash = short_hash(user)
    if hash not in files:
        files[hash] = gzip.open(filebase+'/'+hash, "w")
    files[hash].write(json.dumps(item, sort_keys=True))
    files[hash].write('\n')

for file in files:
    files[file].close()
