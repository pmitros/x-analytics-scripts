import boto.sqs
from boto.sqs.message import Message
from boto.s3.connection import S3Connection

import xanalytics.settings

def sqs_s3_deque():
    '''
    If we have a set of tracking log files on Amazon S3, this lets us
    grab all of the lines, and process them. 

    In most cases, this script would be running in parallel on a
    cluster of machines. This lets us process many files quickly.

    logs_to_sqs.py is a good helper script for setting things up. 

    We do boto imports locally since this file otherwise does not rely
    on AWS.
    '''
    s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
    sqs_conn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])

    q = sqs_conn.get_queue(xanalytics.settings.settings["tracking-logs-queue"])
    file_count = 0
    total_bytes = 0
    while q.count() > 0:
        m = q.read(60*20) # We limit processing to 20 minutes per file
        item = m.get_body()
        file_count = file_count+1
        print item

        source_bucket = s3_conn.get_bucket(xanalytics.settings.settings['tracking-logs-bucket'])
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
    sqs_conn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
    q = sqs_conn.get_queue(xanalytics.settings.settings["tracking-logs-queue"])

    for item in data:
        m = Message()
        m.set_body(item)
        q.write(m)


def list_s3_bucket(prefix = "logs/tracking"):
    '''
    yield a list of all files that start with a given prefix.
    '''
    s3_conn = S3Connection(aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
    bucket = s3_conn.get_bucket(xanalytics.settings.settings["tracking-logs-bucket"])

    sqs_conn = boto.sqs.connect_to_region("us-east-1", aws_access_key_id=xanalytics.settings.settings['edx-aws-access-key-id'], aws_secret_access_key=xanalytics.settings.settings['edx-aws-secret-key'])
    q = sqs_conn.get_queue(xanalytics.settings.settings["tracking-logs-queue"])

    for key in bucket.list():
        if not key.name.startswith(prefix):
            continue
        yield key.name.encode('utf-8')
