import sys
from xanalytics.streaming import *

filename = sys.argv[1]

data = read_bson_file(filename)

for d in data:
    print json.dumps(d)
