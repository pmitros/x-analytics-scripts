'''
This will take a tracking log file and it will output the lines of
that file, sorted, first by username than by date. It takes about 6-7
minutes on a 220MB gzipped log file with 2.7 million event lines on an
Intel i7-4770S.

In the process, it decodes browser events and truncates events over
500 characters.
'''

import gzip
import json
import sys

import xanalytics.megasort as megasort
import xanalytics.streaming as streaming
import xanalytics.xevents as xevents

def key(sample):
    sample = json.loads(sample)
    return sample.get("username", "")+' '+sample.get("time", "")

data_sample = gzip.open(sys.argv[1])
data_sample = streaming.text_to_json(data_sample)
data_sample = xevents.decode_browser_event(data_sample)

data_sample = xevents.truncate_json(data_sample, 500)
data_sample = streaming.json_to_text(data_sample)
data_sample = megasort.megasort(data_sample, key=key)
for x in data_sample:
    print x
