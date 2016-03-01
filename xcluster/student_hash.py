'''
This is a small script which will pull lines out log files, via
Amazon SQS, and write them into files sharded based on user id.

This was part of a workflow to reorganize data on a per-user
basis so we could have learner traces. This might be a little
obsolete right now -- for the most part, we can do this with
megasort. But it is a nice demo of how to do Python Hadoop-like
operations on AWS easily with the framework.
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

if __name__ == '__main__':
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


if __name__ == '__main__':
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
