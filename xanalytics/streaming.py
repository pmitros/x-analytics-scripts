'''
This file contains streaming data processors. Why stream
processing? A few reasons:

1. You get immediate results. Development time is much faster. If you
   have a bug, you see it almost immediately.
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
import itertools
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

from bson import BSON

import xanalytics.settings

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
                o = f(d, *(args + fargs))
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
        warnings.warn("We're deprecating directory support in favor of pyfs.")
        return fs.osfs.OSFS(filesystem_or_directory)
    else:
        raise AttributeError("Unrecognized parameter for filesystem argument: " + repr(filesystem))


def get_files(filesystem, directory=".", only_gz=False):
    '''
    Return an iterator of all the files in a given directory or pyfilesystem.
    >>> "__init__.py" in list(get_files("."))
    True
    >>> import fs.osfs
    >>> "__init__.py" in list(get_files(fs.osfs.OSFS(".")))
    True
    '''
    filesystem = _to_filesystem(filesystem)
    for f in sorted(filesystem.listdir(directory)):
        if only_gz and not f.endswith(".gz"):
            continue
        yield f


def _read_text_data(filesystem, directory=".", only_gz=False):
    '''
    Helper: Yield all the lines in all the files in a directory.

    The only_gz helps with edX directories which contain a mixture of useful and useless files.
    '''
    filesystem = _to_filesystem(filesystem)

    for f in filesystem.listdir(directory):
        if only_gz and not f.endswith(".gz"):
            continue
        for line in filesystem.open(directory + "/" + f):
            yield line.encode('ascii', 'ignore')


def _read_bson_data(filesystem, directory):
    for f in get_files(filesystem, directory, only_gz):
        for line in _read_bson_file(filesystem.open(os.path.join(directory, f))):
            yield line


@filter_map
def text_to_csv(line, csv_delimiter="\t", csv_header=False):
    '''
    Untested
    '''
    if csv_header:
        # TODO
        raise UnimplementedException("CSVs with headers don't work yet. Sorry. Major hole.")
    else:
        if line[-1] == '\n':
            line = line[:-1]
        return line.split(csv_delimiter)


def read_data(filesystem,
              directory=".",
              only_gz=False,
              format="text",
              csv_delimiter="\t",
              csv_header=False):
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

    >>> data = ("""{"a":"b"}""", """["c", "d"]""", )
    >>> list(text_to_json(data))
    [{u'a': u'b'}]
    '''
    line = line.strip()
    if "{" not in line:
        return None
    if len(line) == 0:
        return None
    if line[0] not in ['{']:  # Tracking logs are always dicts
        return None

    if clever:
        endings = ['', '}', '"}', '""}', '"}}', '"}}}', '"}}}}', '"}}}}}', '"}}}}}}']
        for ending in endings:
            try:
                line = json.loads(line + ending)
                return line
            except:
                pass
        print line
        print "Warning: We've got a line we couldn't fix up."
        print "Please look at it, and add the right logic to streaming.py."
        print "It's usually a new ending. Sometimes, it's detecting a non-JSON line of some form"
        os.exit(-1)

    # We've truncated lines to random lengths in the past...
    if len(line) in range(32000, 33000):
        return None
    elif len(line) in range(9980, 10001):
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


def json_to_text(data):
    ''' Convert JSON back to text, for dumping to processed file
    '''
    for line in data:
        yield json.dumps(line) + '\n'


_data_part = 0
_data_item = 0
def save_data(data, directory):
    '''Write data back to the directory specified. Data is dumped into
    individual files, each a maximum of 20,000 events long (by
    default, overridable in settings).
    '''
    global _data_part, _data_item
    fout = None
    max_file_length = int(settings.settings.get('max-file-size', 20000))
    for line in data:
        if _data_item % 20000 == 0:
            if fout:
                fout.close()
            fout = gzip.open(directory + '/part' + str(_data_part) + ".gz", "w")
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
def short_hash(string, length=3, memoize=False):
    '''
    Provide a compact hash of a string. Returns a hex string which is
    the hash. length is the length of the string.

    This is helpful if we want to shard data. This is not helpful if
    we want to avoid collisions. The hash is **short**.

    >>> short_hash("Alice") != short_hash("Bob")
    True
    >>> short_hash("Eve") == short_hash("Eve")
    True
    >>> "Alice" not in _hash_memory
    True
    >>> short_hash("Eve", memoize=True) == short_hash("Eve", memoize=True)
    True
    >>> "Eve" in _hash_memory
    True
    >>> len(short_hash("Mallet")) == 3
    True
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


def list_short_hashes(length):
    '''
    A generator of all hashes of length `length` (as would be generated by short_hash)

    Hashes are of the form we expect
    >>> "aa" in list(list_short_hashes(2))
    True

    They take all possible hex values
    >>> len(list(list_short_hashes(3))) == 16 ** 3
    True

    The list is complete/unique
    >>> len(list(list_short_hashes(3))) == len(set(list_short_hashes(3)))
    True
    >>> short_hash("Hello", 3) in list(list_short_hashes(3))
    True
    '''
    generator = ("".join(x) for x in itertools.product("0123456789abcdef", repeat=length))
    return generator


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
    return sorted(list(data), cmp=event_cmp)


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
        if len(l) < 4:
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
    >>> names = map(str, range(100))
    >>> tokenized_once = map(token, names)
    >>> tokenized_twice = map(token, names)

    Confirm we have the same mapping if we pass a name through twice
    >>> print tokenized_once == tokenized_twice
    True

    Confirm we have a different mapping for different users
    >>> len(set(tokenized_once)) == len(tokenized_once)
    True
    '''
    global _tokens, _token_ct
    if user in _tokens:
        return _tokens[user]
    t = string.letters[(_token_ct / (52 * 52)) % 52] + string.letters[(_token_ct / 52) % 52] + string.letters[_token_ct % 52]
    _token_ct = _token_ct + 1
    if _token_ct > 140607:
        raise "We need to clean up tokenization code to support more users"
    _tokens[user] = t
    return t


def merge_generators(l, key=lambda x: x):
    '''
    Perform a merge of generators, keeping order.

    If inputs are sorted from greatest to least, output will be sorted likewise.

    Possible uses:
    * Hadoop-style merge sort.
    * In-order output from multiprocess.py.

    This is likely obsoleted for most purposes by megasort.py

    >>> import random
    >>> a = sorted([random.randint(0, 50) for x in range(10)], reverse=True)
    >>> b = sorted([random.randint(0, 50) for x in range(10)], reverse=True)
    >>> c = sorted([random.randint(0, 50) for x in range(10)], reverse=True)
    >>> list(merge_generators([a, b, c])) == sorted(a + b + c, reverse=True)
    True
    '''
    l = map(iter, l)

    def next(g):
        '''
        Get next iterm from a generator.

        If no more items, return None
        '''
        try:
            return g.next()
        except StopIteration:
            return None

    def key_wrapper(a):
        if a == None:
            return None
        else:
            return key(a[1])

    heads = [(i, l[i].next()) for i in range(len(l))]

    while max(heads, key=key_wrapper)[1] != None:
        item = max(heads, key=key_wrapper)
        yield item[1]
        heads[item[0]] = (item[0], next(l[item[0]]))

def fields(d, separator="."):
    '''
    Return all of the fields in a JSON object, flattened.

    >>> fields({'a':{'b':'c', 'd':{'e':'f','g':'h'},'i':'j'}, 'k':'l'})
    set(['k', 'a.d.g', 'a.i', 'a.b', 'a.d.e'])
    '''
    s = set()
    for k in d:
        if isinstance(d[k], dict):
            s.update(k+separator+x for x in fields(d[k]))
        else:
            s.add(k)
    return s

def flatten(d, separator="."):
    '''
    Flatten a JSON object, so there is one top-level dictionary, and
    keys are flattened.

    >>> flatten({'a':{'b':'c', 'd':{'e':'f','g':'h'},'i':'j'}, 'k':'l'})
    {'k': 'l', 'a.d.g': 'h', 'a.i': 'j', 'a.b': 'c', 'a.d.e': 'f'}
    '''
    nd = dict()
    for k in d:
        if isinstance(d[k], dict):
            cd = flatten(d[k])
            nd.update((k+separator+x, cd[x]) for x in cd)
        else:
            nd[k] = d[k]
    return nd

def snoop(data):
    for item in data:
        print item
        yield item

if __name__ == '__main__':
    import doctest
    doctest.testmod()
