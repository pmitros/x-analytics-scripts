'''
This is an obsolete way of managing configuration files
'''


import os.path
import json
import yaml

settings_files = ['/etc/edxconfig', os.path.join(os.path.expanduser('~'), ".edx")]

settings = {}

for file in settings_files:
    if os.path.exists(file):
        try:
            settings.update(yaml.load(open(file)))
        except ValueError:
            raise ValueError("Could not parse settings file "+file)

def setting(key):
    ''' Return an edX setting
    '''
    return settings[key]
