"""
 Helper functions for doing big-data work on clusters on AWS.

 Basic trick:
 * List tracking log files into SQS (by filename):
   `xanalytics.aws.sqs_enque(xanalytics.aws.list_s3_bucket())`
 * Spun up a cluster of machines
 * Do normal streaming operations on those machines, grabbing lines
   from those files:
   `data = xanalytics.aws.sqs_s3_deque_lines()`
* Store results locally, and sync back to AWS (this should be moved
  into Python, but for now, just s3cmd sync)
"""

import gzip
import os
import time
import uuid

import boto.sqs
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

from xanalytics.settings import settings


def sqs_s3_deque_lines():
    '''
    If we have a set of tracking log files on Amazon S3, this lets us
    grab all of the lines, and process them.

    In most cases, this script would be running in parallel on a
    cluster of machines. This lets us process many files quickly.

    logs_to_sqs.py is a good helper script for setting things up.

    We do boto imports locally since this file otherwise does not rely
    on AWS.
    '''
    s3_conn = S3Connection(
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    sqs_conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )

    q = sqs_conn.get_queue(settings["tracking-logs-queue"])
    file_count = 0
    total_bytes = 0
    while q.count() > 0:
        m = q.read(60 * 20)  # We limit processing to 20 minutes per file
        item = m.get_body()
        file_count = file_count + 1
        print item

        source_bucket = s3_conn.get_bucket(settings['tracking-logs-bucket'])
        key = source_bucket.get_key(item)
        filename = "/tmp/log_" + uuid.uuid1().hex + ".log"
        key.get_contents_to_filename(filename)
        try:
            lines = gzip.open(filename).readlines()
        except IOError:
            lines = open(filename).readlines()
        for line in lines:
            yield line

        total_bytes = total_bytes + key.size
        if True:
            print file_count, "files", item, total_bytes / 1.e9, "GB"
        q.delete_message(m)

        os.unlink(filename)


def sqs_enque(data):
    '''
    Send (string) data to an Amazon SQS queue.

    Good uses:
    * Send a list of filenames of tracking files stored on S3.
      ~50k requests for around 3 pennies.

    Bad uses:
    * Send all events to Amazon SQS as part of a pipeline. Each
      billion events runs us $500.

    TODO: Batch operations. Would improve performance and cut cost
    10x.

    (Untested)
    '''
    sqs_conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    q = sqs_conn.get_queue(settings["tracking-logs-queue"])

    for item in data:
        m = Message()
        m.set_body(item)
        q.write(m)


def list_s3_bucket(bucket_key='tracking-logs-bucket', prefix="logs/tracking"):
    '''
    yield a list of all files that start with a given prefix.
    '''
    s3_conn = S3Connection(
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    bucket = s3_conn.get_bucket(settings[bucket_key])

    sqs_conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    q = sqs_conn.get_queue(settings["tracking-logs-queue"])

    for key in bucket.list():
        if not key.name.startswith(prefix):
            continue
        yield key.name.encode('utf-8')


def get_hashed_files(name,
                     prefixes,
                     bucket_key='scratch-bucket',
                     delete=False):
    '''
    Grab all files on AWS which were generated with `streaming.short_hash()`.

    This is helpful for Hadoop-style operations. Each server on a
    server farm can generate independent files with a hash of the key. The
    next set of servers can grab the set of files they want to process.

    Example:

    If prefixes is: '/log/1', '/log/2', etc.
    If name is: 'aaa'

    This will iterate through all of the files named '/log/1/aaa',
    '/log/2/aaa', etc., if they exist, and return an iterator over all
    of the lines in those files.

    If delete = True, it will delete those files once it is done with
    /all/ of them

    '''
    s3_conn = S3Connection(
        aws_access_key_id=settings['edx-aws-access-key-id'],
        aws_secret_access_key=settings['edx-aws-secret-key']
    )
    bucket = s3_conn.get_bucket(settings[bucket_key])

    good_keys = list()

    filename = "/tmp/log_" + uuid.uuid1().hex + ".log"

    for prefix in prefixes:
        key_name = os.path.join(prefix, name)
        key = bucket.get_key(key_name)
        if key:
            key.get_contents_to_filename(filename)

            try:
                lines = gzip.open(filename).readlines()
                print "No IOError", key_name
            except IOError:
                print "IOError", key_name
                lines = open(filename).readlines()
            for line in lines:
                yield line

            good_keys.append(key_name)

    os.unlink(filename)

    if delete:
        bucket.delete_keys(good_keys)


def wait_for_spot_instance_fulfillment(conn, request_ids):
    """
    Wait until request for AWS spot instances have been fulfilled.

    Loop through all pending request ids waiting for them to be
    fulfilled.  If there are still pending requests, sleep and check
    again in 10 seconds.  Only return when all spot requests have been
    fulfilled.

    This is a bad idea for automated tasks as-is. AWS can choose to,
    for example, fill five requests, and hold off on one. In such a
    case, this won't return (since not all have been fulfilled), but
    you'll still be paying for the instances which were. Using spot
    instances in automated tasks requires a lot more in terms of
    timeouts, error-handling logic, etc.

    """
    def request_state():
        request_ids = [r.id for r in request_ids]
        return [s.state
                for s
                in conn.get_all_spot_instance_requests(request_ids)]

    while 'open' in request_state():
        time.sleep(10)
        print request_state()
    instance_ids = [r.instance_id
                    for r
                    in conn.get_all_spot_instance_requests(
                        [r.id for r in request_ids]
                    )]
    instances = sum(
        [list(r.instances)
         for r
         in conn.get_all_instances(instance_ids)],
        []
    )
    ips = [i.ip_address for i in instances]
    return ips

'''
Example:
l = list(list_s3_bucket('scratch-bucket', prefix=""))
prefixes = list(
  set(["/".join(i.split('/')[:-1])
       for i
       in l
       if 'forums' not in i and 'sample-data' not in i
  ]))
users = field_set(
  text_to_json(
    xanalytics.aws.get_files("aaa", prefixes)
  ),
  "username"
)
'''
