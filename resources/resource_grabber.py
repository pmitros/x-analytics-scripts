'''
This script will grab all edX problems from a Mongo database, and
export them as individual files in a directory. This is *NOT TESTED
WITH SPLIT MONGO*. It is likely to explode after this change. 

Run as: 
  python resource_grabber.py username password problem mongodb://database.24.mongolayer.com:27107/my-clone-database") my-clone-database

Or:
  python problem

This script is untested in the current incarnation! However, the
only change since the tested incarnation is that:

  client = MongoClient(sys.argv[1])
  db = client[sys.argv[2]]

Used to be hardcoded to edX production servers.
'''

import argparse
import json
import sys
from xanalytics import settings

from pymongo import MongoClient

print settings.settings._settings
print ("mongo-replica-url" in settings.settings)
print len(sys.argv)
print settings.settings['mongo-replica-url']

if ("mongo-replica-url" in settings.settings) and len(sys.argv) < 3:
    url = settings.settings["mongo-replica-url"]
    username = settings.settings["mongo-replica-username"]
    password = settings.settings["mongo-replica-password"]
    db = settings.settings["mongo-replica-database"]
    resource_type = sys.argv[1]
else:
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("resource_type")
    parser.add_argument("url")
    parser.add_argument("database")
    parser.parse_args()
    url = parser.url
    username = parser.username
    password = parser.password
    db = parser.database
    resource_type = parser.resource_type

client = MongoClient(url)
db = client[db]
collection = db['modulestore']
db.authenticate(username, password)
c = collection.find({"_id.category":resource_type})
for item in c:
    id = item['_id']
    filename = resource_type+u"/"+(u"{tag}.{org}.{course}.{name}.{category}".format(**id).replace("/","_"))
    d = item['definition']['data']
    md = json.dumps(item['metadata'])
    if isinstance(d,dict) and 'data' in d:
        d = d['data']
    if not isinstance(d, basestring):
        d = json.dumps(d)
    with open(filename+".data", "w") as f:
        f.write(d.encode('utf8'))
    with open(filename+".metadata", "w") as f:
        f.write(md.encode('utf8'))
