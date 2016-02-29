'''
This is used to explore the schema of an event log. It shows all the
event fields which occur in a file, and an examplar of what's in those
fields.

It is currently in flux; probably won't work
'''

import gzip
import json
import sys

import xanalytics.streaming as streaming
import xanalytics.xevents as xevents


data_sample = gzip.open(sys.argv[1])
data_sample = streaming.text_to_json(data_sample)
data_sample = xevents.decode_browser_event(data_sample)

field_set = dict()
for j in data_sample:
    new_fields = streaming.fields(j)
    fj = streaming.flatten(j)
    field_set.update((x, fj[x])
                     for x in new_fields
                     if 'correct_map.' not in x and
                        'submission' not in x and
                        'answers' not in x and
                        'POST' not in x)

print "\n".join("\t".join([x, field_set[x]]) for x in sorted(field_set))
