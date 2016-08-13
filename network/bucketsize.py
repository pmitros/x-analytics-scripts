''' This is both a script and a library to get the size of directories in S3 buckets. 

As a script, run: 

  python bucketsize.py edx-data/pmitros/6.002x edx-data/pmitros/hinter

or

  python bucketsize.py edx-data

As a library, just call bucketsize.bucketsize.
'''

import sys
import boto
conn = boto.connect_s3()
total = 0

def bucketsize(path):
    bucket = path.split('/')[0]
    s3bucket = conn.get_bucket(bucket)
    path = path.split('/')[1:]
    prefix = "/".join(path)
    files = s3bucket.list(prefix=prefix)
    size = sum(t.size for t in files)
    return size

if __name__=="__main__":
    for x in sys.argv[1:]:
        size = bucketsize(x)
        print x, size
        total = total+size

    print "Total: ", total, "bytes", total/1.e3, "kB", total/1.e6, "MB", total/1.e9, "GB", total/1.e12, "TB", 
