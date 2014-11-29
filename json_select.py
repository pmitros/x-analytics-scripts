'''
Select items from json dictionary. 

Items are passed in to stdin

Fields to select are passed in on the commandline. 

Please note this script is likely to change syntax.
'''

import json
import sys

for line in sys.stdin:
    try:
        line = json.loads(line)
    except ValueError:
        continue
    if not isinstance(line, dict):
        continue
    for arg in sys.argv:
        if arg in line:
            print line[arg],
    print
