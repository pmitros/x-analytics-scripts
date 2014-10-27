'''This is a script which will help clean and desensitize edX data. 

Note that the data is *not* deidentified or anonymized. It would be
relatively easy to take desensitized data and map it back to usernames
an similar.

Desensitization removes *most* personally identifiable information
(not all). This prevents certain classes of casual mistakes and leaks:

* I'm looking through a log file, and I see someone I know. 
* I'm working on data. Someone passes behind me and glances at my
  screen.
* I'm working on a presentation. I accidentally use usernames for 
  major points in a sociogram.
* Etc.

The specific approach this code takes is:

* Most PII and similar not needed for analytics is removed (e.g. IP addresses)
* Usenames are replaced with anonymous tokens

In addition, this script: 
* Cleans and shrinks logs, such that they are easier to work with.
* Filters event files.

Word of warning: I did some moving back and forth between files. This
script worked before; if it doesn't work, it's something simple (like
a missing import). 

'''

import argparse
import dateutil.parser
import os
import gzip
import json
import string
import cjson

from streaming import *

parser = argparse.ArgumentParser(description='Desensitize (but not anonymize) and clean edX data.')
parser.add_argument("input", help="Input directory")
parser.add_argument("output", help="Output directory")
parser.add_argument("--mindate", help="Date cutoff", default=None)
args = parser.parse_args()

print "Reading from ", args.input
data = read_data(args.input)
print "Writing to ", args.output
data = text_to_json(data)
if args.mindate:
    data = date_gt_filter(data, args.mindate)
data = decode_event(data)
data = remove_redundant_data(data)
data = desensitize_data(data, ['agent', 'ip', 'host', 'user_id', "session"], ["csrfmiddlewaretoken", "session"])
data = json_to_text(data)
save_data(data, args.output)
