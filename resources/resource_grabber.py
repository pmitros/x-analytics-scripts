## This script will grab all edX problems from a Mongo database, and
## export them as individual files in a directory. This is *NOT TESTED
## WITH SPLIT MONGO*. It is likely to explode after this change. 
##
## Run as: 
##  python resource_grabber.py problem mongodb://database.24.mongolayer.com:27107/my-clone-database") my-clone-database
##
## This script is untested in the current incarnation! However, the
## only change since the tested incarnation is that:
##
##  client = MongoClient(sys.argv[1])
##  db = client[sys.argv[2]]
##
## Used to be hardcoded to edX production servers.

import json
import sys
from pymongo import MongoClient

client = MongoClient(sys.argv[2])
db = client[sys.argv[3]]
resource_type = sys.argv[1]
collection = db['modulestore']
db.authenticate(sys.argv[1], sys.argv[2])
c = collection.find({"_id.category":resource_type})
for item in c:
    id = item['_id']
    filename = resource_type+u"/"+(u"{tag}.{org}.{course}.{name}.{category}".format(**id).replace("/","_"))
    d = item['definition']['data']
    md = json.dumps(item['metadata'])
    if type(d) == dict:
        d = d['data']
    with open(filename+".data", "w") as f:
        f.write(d.encode('utf8'))
    with open(filename+".metadata", "w") as f:
        f.write(md.encode('utf8'))
