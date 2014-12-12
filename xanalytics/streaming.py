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
import gzip
import md5
import numbers
import os
import string
import struct
import warnings

try:
    import simplejson as json
except:
    import json


from fs.base import FS

import fs.osfs

from bson import BSON  #  from pymongo in pip


######
## Generic functions to stream processing in Python
######


def filter_map(f, *args):
    '''
    Turn a function into an iterator and apply to each item. Pass on
    items which return 'None'
    '''
    def map_stream(data, *fargs):
        for d in data:
            if d is not None:
                o = f(d, *(args+fargs))
                if o:
                    yield o 
    return map_stream


def _to_filesystem(filesystem_or_directory):
    '''
    Take a pyfilesystem or directory. 

    If a directory, return a pyfilesystem. 

    This gives backwards-compatibility with code before we moved to pyfs
    '''
    if isinstance(filesystem_or_directory, FS):
        return filesystem_or_directory
    elif isinstance(filesystem_or_directory, basestring):
        warnings.warn("Warning: We're deprecating directory support in favor of pyfs.")
        return fs.osfs.OSFS(filesystem_or_directory)
    else:
        raise AttributeError("Unrecognized parameter for filesystem argument: "+repr(filesystem))


def get_files(filesystem, directory = ".", only_gz = False):
    '''
    Return an iterator of all the files in a given directory or pyfilesystem.     
    '''
    filesystem = _to_filesystem(filesystem)
    for f in sorted(filesystem.listdir(directory)):
        if only_gz and not f.endswith(".gz"):
            continue
        yield f


if __name__=="__main__":
    # Confirm we can list the current directory, either as a string, or as a pyfilesystem
    print "__init__.py" in list(get_files("."))
    import fs.osfs
    print "__init__.py" in list(get_files(fs.osfs.OSFS(".")))


def _read_text_data(filesystem, directory = ".", only_gz = False):
    '''
    Helper: Yield all the lines in all the files in a directory.

    The only_gz helps with edX directories which contain a mixture of useful and useless files.
    '''
    filesystem = _to_filesystem(filesystem)

    for f in filesystem.listdir(directory):
        if only_gz and not f.endswith(".gz"):
            continue
        for line in filesystem.open(directory+"/"+f):
            yield line.encode('ascii', 'ignore')


def _read_bson_data(filesystem, directory):
    for f in get_files(filesystem, directory, only_gz):
        for line in _read_bson_file(filesystem.open(os.path.join(directory, f))):
            yield line


@filter_map
def text_to_csv(line, csv_delimiter="\t", csv_header = False):
    '''
    Untested
    '''
    if csv_header:
        # TODO
        raise UnimplementedException("CSVs with headers don't work yet. Sorry. This is kind of a major hole.")
    else:
        if line[-1] == '\n':
            line = line[:-1]
        return line.split(csv_delimiter)


def read_data(filesystem, directory = ".", only_gz = False, format="text", csv_delimiter="\t", csv_header = False):
    '''Takes a pyfs containing log files. Returns an iterator of all
    lines in all files.

    Optional: Skip non-.gz files.
    Optional: Format can be text, JSON, or BSON, in which case, we'll decode. 
    '''
    filesystem = _to_filesystem(filesystem)
    if format == "text":
        return _read_text_data(filesystem, directory, only_gz)
    elif format == "json":
        return text_to_json(_read_text_data(filesystem, directory, only_gz))
    elif format == "bson":
        warnings.warn("Untested code path")
        return _read_bson_data(filesystem, directory)
    elif format == "csv":
        return text_to_csv(_read_text_data(filesystem, directory, only_gz), csv_delimiter, csv_header)
    else: 
        raise AttributeError("Unknown format: ", format)


@filter_map
def text_to_json(line, clever=False):
    '''Decode lines to JSON. If a line is truncated, this will drop the line. 
    
    clever allows us to try to reconstruct long lines. This is not
    helpful for most analytics due to performance, but it is in cases
    where we need every last bit of data.

    '''
    line = line.strip()
    if "{" not in line:
        return None
    if len(line) == 0:
        return None
    if line[0] not in ['{']: # Tracking logs are always dicts
        return None

    if clever:
        endings = ['', '}', '"}', '""}', '"}}', '"}}}', '"}}}}', '"}}}}}', '"}}}}}}']
        for ending in endings: 
            try:
                line = json.loads(line+ending)
                return line
            except:
                pass
        print line
        print "Warning: We've got a line we couldn't fix up. Please look at it, and add the right logic to streaming.py."
        print "It's usually a new ending. Sometimes, it's detecting a non-JSON line of some form"
        os.exit(-1)

    # We've truncated lines to random lengths in the past... 
    if len(line) in range(32000, 33000):
        return None
    elif len(line) in range(9980,10001):
        return None
    elif len(line) in range(2039, 2044):
        return None
    try:
        line = json.loads(line)
        return line
    except ValueError:
        print line, len(line)
        return None
        #raise


if __name__ == '__main__':
    data = ("""{"a":"b"}""","""["c","d"]""",)
    print list(text_to_json(data)) == [{u'a': u'b'}]


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


def read_bson_file(filename):
    '''
    Reads a dump of BSON to a file.

    Reading BSON is 3-4 times faster than reading JSON with:
      import json

    Performance between cjson, simplejson, and other libraries is more
    mixed.

    Untested since move from filename to fp and refactoring
    '''
    return _read_bson_file(gzip.open(filename))


def encode_to_bson(data):
    '''
    Encode to BSON. Encoding BSON is about the same speed as encoding
    JSON (~25% faster), but decoding is much faster. 
    '''
    for d in data:
        yield BSON.encode(d)


_hash_memory = dict()
def short_hash(string, length=3, memoize = False):
    '''
    Provide a compact hash of a string. Returns a hex string which is
    the hash. length is the length of the string. 

    This is helpful if we want to shard data.
    '''
    global _hash_memory
    if memoize:
        if string in _hash_memory: 
            return _hash_memory[string]
    m = md5.new()
    m.update(string)
    h = m.hexdigest()[0:length]
    if memoize:
        _hash_memory[string] = h
    return h

if __name__=='__main__':
    print hash("Alice") != hash("Bob")
    print hash("Eve") == hash("Eve")
    print "Alice" not in _hash_memory
    print hash("Eve", memoize=True) == hash("Eve", memoize=True)
    print "Eve" in _hash_memory
    print len(hash("Mallet")) == 3


def sqs_lines():
    '''
    If we have a set of tracking log files on Amazon S3, this lets us
    grab all of the lines, and process them. 

    In most cases, this script would be running in parallel on a
    cluster of machines. This lets us process many files quickly.

    logs_to_sqs.py is a good helper script for setting things up. 

    We do boto imports locally since this file otherwise does not rely
    on AWS.
    '''
    import boto.sqs
    from boto.s3.connection import S3Connection

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


def filter_data(data, filter):
    '''
    Apply a function 'filter' to all elements in the data
    '''
    for item in data:
        if filter(item):
            yield item

######
##  Stream operations specific to edX
######


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
            if 'context' in line and item in list(line['context']):
                del line['context'][item]
        if 'context' in line and len(line['context']) == 0:
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


def truncate_json(data, max_length):
    for d in data:
        t = _truncate_json(d, max_length)
        yield t


def _truncate_json(data_item, max_length):
    '''
    Truncate strings longer than max_length in a JSON object. Long
    strings are replaced with 'none'
    '''
    if isinstance(data_item, dict):
        for key in data_item:
            data_item[key] = _truncate_json(data_item[key], max_length)
        return data_item
    elif isinstance(data_item, numbers.Number):
        return data_item
    elif isinstance(data_item, basestring):
        if len(data_item)>max_length:
            return None
        return data_item
    elif isinstance(data_item, list):
        return list(map(lambda x:_truncate_json(x, max_length), data_item))
    elif data_item is None:
        return data_item
    else:
        raise AttributeError


def memoize(data, directory):
    '''
    UNTESTED/UNTESTED/UNTESTED

    Check if the directory already has data. If so, read it in. Otherwise, dump 
    data to the directory and return it. 

    The current version reads/writes redundantly on the save operation, and 
    JSON decodes redundantly, so it's not very performant. This would be worth
    fixing. 

    UNTESTED/UNTESTED/UNTESTED
    '''
    for f in os.listdir(directory):
        if not f.endswith(".gz"):
            continue
        return read_data(directory)

    save_data(data, directory)
    return read_data(directory)


def _read_bson_file(fp):
    while True:
        l = f.read(4)
        if len(l)<4:
            break
        length = struct.unpack('<i', l)
        o = l + f.read(length[0]-4)
        yield BSON.decode(BSON(o))


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


if __name__=="__main__":
    names = map(str, range(100))
    tokenized_once = map(token, names)
    tokenized_twice = map(token, names)
    # Confirm we have the same mapping if we pass a name through twice
    print tokenized_once == tokenized_twice
    # Confirm we have a different mapping for different users
    print len(set(tokenized_once)) == len(tokenized_once)


