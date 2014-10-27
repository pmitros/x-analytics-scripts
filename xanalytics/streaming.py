'''
This file contains streaming data processors. Why stream processing? A few reasons:

1. You get immediate results. Development time is much faster. If you have a bug, you see it almost immediately.
2. The code is more readable.

Downsides:

1. Stack traces are a bit ugly.

See:
  http://www.dabeaz.com/generators/Generators.pdf

For best practices.
'''


import argparse
import dateutil.parser
import os
import gzip
import json
import string
import cjson


_tokens = dict()
_token_ct = 0
def token(user):
    ''' Generate a token for a username. The tokens are generated in order, so this is 
    not generically secure. In this context, they are generate by the order users appear in the 
    log file. '''
    global _tokens, _token_ct
    if user in _tokens:
        return _tokens[user]
    t = string.letters [ (_token_ct / (52*52)) % 52]  + string.letters [ (_token_ct / 52) % 52]  + string.letters [ _token_ct % 52]
    _token_ct = _token_ct + 1
    _tokens[user] = t
    return t


def read_data(directory):
    '''Takes a directory containing log files. Returns an iterator of all
    lines in all files.
    '''
    for f in os.listdir(directory):
        for line in gzip.open(directory+"/"+f):
            yield line


def text_to_json(data):
    ''' Decode lines to JSON. If a line is truncated, this will drop the line. 
    '''
    for line in data:
        line = line.strip()
        if len(line) in range(32000, 33000):
            continue
        try:
            line = json.loads(line)
            yield line
        except ValueError:
            print line, len(line)
            raise


def decode_event(data):
    ''' Convert browser events from string to JSON
    '''
    for line in data:
        if 'event' in line:
            try:
                while isinstance(line['event'], basestring):
                    line['event'] = json.loads(line['event'])
            except ValueError:
                line['event'] = 'Truncated'
            yield line


def desensitize_data(data, sensitive_fields, sensitive_event_fields):
    ''' Remove known-sensitive fields.
    '''
    for line in data:
        for item in sensitive_fields:
            if item in line:
                del line[item]
            if 'context' in line and item in line['context']:
                del line['context'][item]
        for item in sensitive_event_fields:
            if 'event' in line and item in line["event"]:
                del line["event"]["item"]

        if 'username' in line:
            line['username'] = token(line['username'])
        yield line


def remove_redundant_data(data):
    '''Some versions of edX would put event data both at the top-level
    and in context. This cleans this up.
    '''
    for line in data:
        if 'context' in line:
            for item in list(line['context']):
                if item in line and line[item] == line['context'][item]:
                    del line['context'][item]
        for item in ['course_user_tags']:
            if item in list(line['context']):
                del line['context'][item]
        if len(line['context']) == 0:
            del line['context']
        yield line


def date_gt_filter(data, date):
    ''' Filter data based on date. Date is a pretty free-form string format. If date is None, this is a no-op. 
    '''
    if not date:
        for line in data:
            yield line

    date = dateutil.parser.parse(date)
    for line in data:
        if dateutil.parser.parse(line["time"]) > date:
            yield line


def json_to_text(data):
    ''' Convert JSON back to text, for dumping to processed file
    '''
    for line in data:
        yield json.dumps(line)+'\n'


_data_part = 0
_data_item = 0
def save_data(data, directory):
    '''Write data back to the directory specified. Data is dumped into
    individual files, each a maximum of 20,000 events long. 
    '''
    global _data_part, _data_item
    fout = None
    for line in data:
        if _data_item % 20000 == 0:
            if fout:
                fout.close()
            fout = gzip.open(directory+'/part'+str(_data_part)+".gz", "w")
            _data_part = _data_part + 1
        fout.write(line)
        _data_item = _data_item + 1

