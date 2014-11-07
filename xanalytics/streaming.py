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


_tokens = dict()
_token_ct = 0
def token(user):
    '''
    Generate a token for a username. The tokens are generated in order, so this is 
    not generically secure. In this context, they are generate by the order users appear in the 
    log file. 

    Note that this is limited to courses with 140608 users for now (if
    you go over, it will raise an exception).
    '''
    global _tokens, _token_ct
    if user in _tokens:
        return _tokens[user]
    t = string.letters [ (_token_ct / (52*52)) % 52]  + string.letters [ (_token_ct / 52) % 52]  + string.letters [ _token_ct % 52]
    _token_ct = _token_ct + 1
    if _token_ct > 140607:
        raise "We need to clean up tokenization code to support more users"
    _tokens[user] = t
    return t


def read_data(directory):
    '''Takes a directory containing log files. Returns an iterator of all
    lines in all files.

    Skip non-.gz files.
    '''
    for f in os.listdir(directory):
        if not f.endswith(".gz"):
            continue
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
    '''Remove known-sensitive fields and replace usernames with tokens.

    This does not fully deidentify data. It is helpful, however, for
    preventing a range of simple slip-ups in data handling. 
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
    and in context. This cleans this up by removing it from context.
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


def event_count(data):
    '''
    Count number of events in data.
    '''
    count = 0
    for event in data:
        count = count + 1
    return count


def users_count(data):
    '''
    Count number of unique users in data
    '''
    return len(field_set(data, "username"))


def field_set(data, field):
    '''
    Return a set of unique items in field
    '''
    us = set()
    for event in data:
        us.add(__select_field(event, field))
    return us


def filter_on_events(data, event_types):
    '''
    Filter data based on event types
    '''
    for d in data:
        if d['event_type'] in event_types:
            yield d



def filter_on_courses(data, courses):
    '''
    Filter data based on event types
    '''
    for d in data:
        if 'context' in d and \
           'course_id' in d['context'] and \
           d['context']['course_id'] in courses:
            yield d


def filter_on_fields(data, field_spec):
    '''
    Filter through fields
    
    field_spec maps field names to lists of possible values. For example:
    {'username':['jack','jill']
    Will return all of the data where the user is either Jack or Jill
    '''
    for d in data:  # d is the event
        valid = True  # Does the event match the spec?
        for field in field_spec:  # Field we're looking at
            value = __select_field(d, field)
            if value not in field_spec[field]:
                valid = False
        if valid:
            yield d


def dbic(data, label):
    '''
    Debug: Print item count
    '''
    cnt = 0
    for d in data:
        cnt = cnt + 1
        yield d
    print label, cnt


def __select_field(event, field):
    '''
    Takes a field definition and a dictionary. Does a hierarchical query.
    __select_field(event, "event:element") is equivalent to: 
    try:
      event['event']['element']
    except KeyError:
      return None
    '''
    for key in field.split(":"):  # Pick out the hierarchy 
        if key not in event:
            return None
        event = event[key]
    return event


def sort_events(data, fields):
    '''
    Sort data. Warning: In-memory. Not iterable. Only works for small datasets
    '''
    def event_cmp(d1, d2):
        for field in fields:
            c = cmp(__select_field(d1, field), __select_field(d2, field))
            if c != 0:
                return c
        return 0
    return sorted(list(data), cmp = event_cmp)


def select_fields(data, fields):
    '''
    Filter data down to a subset of fields. Also, flatten (should be a param in the future whether to do this.
    '''
    for d in data:
        d2 = {}
        for field in fields:
            d2[field] = __select_field(d, field)
        yield d2


def select_in(data, string):
    '''
    Select data from _text_ (not JSON) where a string appears is in the data
    '''
    for d in data:
        if string in d:
            yield d


def filter_data(data, filter):
    '''
    Apply a function 'filter' to all elements in the data
    '''
    for item in data:
        if filter(item):
            yield item


def truncate_json(data_item, max_length):
    '''
    Truncate strings longer than max_length in a JSON object. Long
    strings are replaced with 'none'
    '''
    if isinstance(data_item, dict):
        for key in data_item:
            data_item[key] = truncate_json(data_item[key], max_length)
        return data_item
    elif isinstance(data_item, numbers.Integral):
        return data_item
    elif isinstance(data_item, basestring):
        if len(data_item)>max_length:
            return None
        return data_item
    elif isinstance(data_item, list):
        return map(lambda x:truncate_json(x, max_length), data_item)
    else:
        raise AttributeError
