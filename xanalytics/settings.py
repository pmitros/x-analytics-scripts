import os.path
import pkg_resources
import yaml

import fs.osfs

from xanalytics.gzipfs import GZIPFS

settings_files = ['/etc/xanalytics', '~/.xanalytics']


class _settings(object):
    '''
    This is a special dictionary-like object which will merge settings from one or more files
    on disk, and combine them in order. For example, we might have a system-wide set
    of defaults, a configuration file in `/etc`, a local user override
    for some settings, a command-line override for others, and even
    test/debug hotpatches from there. Right now, these files are
    `/etc/xanalytics` and `~/.xanalytics`. 

    Settings overrides can be helpful for things like commandline
    parameters, as well as patching for test cases. The logic for
    overrides is as follows:

    >>> initial_max = settings.get("max-file-size")
    >>> settings.push_overrides({'max-file-size': 5})
    >>> settings['max-file-size']
    5
    >>> settings.push_overrides({'max-file-size': 10})
    >>> settings['max-file-size']
    10
    >>> settings.pop_overrides()
    {'max-file-size': 10}
    >>> settings['max-file-size']
    5
    >>> settings.pop_overrides()
    {'max-file-size': 5}
    >>> settings['max-file-size'] == initial_max
    True
    '''
    _settings = None

    _settings_overrides = []

    def refresh(self):
        '''
        Reload data from the settings files on disk.
        '''
        self._settings = dict()
        for f in settings_files:
            f = os.path.expanduser(f)
            if os.path.exists(f):
                self._settings.update(yaml.load(open(f)))

    def __getitem__(self, key):
        for override in self._settings_overrides:
            if key in override:
                return override[key]

        if key in self._settings:
            return self._settings[key]
        else:
            print "WARNING: Misconfigured missing key", key
            print "x-analytics-scripts has a config YAML file"
            print "You should add this key there"
            return None

    def __contains__(self, key):
        for override in self._settings_overrides:
            if key in override:
                return True
        return key in self._settings

    def push_overrides(self, overrides):
        '''
        Override settings. Useful for test cases. Takes a dictionary of
        new settings. Those override the old settings. pop returns
        things back to normal.
        '''
        self._settings_overrides.insert(0, overrides)

    def pop_overrides(self):
        return self._settings_overrides.pop(0)

    def get(self, item, default=None):
        if item in self:
            return self[item]
        else:
            return default

settings = _settings()

settings.refresh()


def _fslookup(namespace, directory, compress):
    basepath = settings[directory]
    if namespace:
        path = os.path.join(basepath, namespace)
        if not os.path.exists(path):
            os.mkdir(path)
    else:
        path = basepath

    if compress:
        return GZIPFS(path)
    else:
        return fs.osfs.OSFS(path)


def outputfs(namespace=False, compress=True):
    return _fslookup(namespace, 'output-dir', compress)


def scratchfs(namespace=False, compress=True):
    return _fslookup(namespace, 'scratch-dir', compress)


def publicdatafs(namespace=False, compress=True):
    public_data_dir = pkg_resources.resource_filename(
        'xanalytics',
        'public_data'
    )
    return fs.osfs.OSFS(public_data_dir)
    #return _fslookup(namespace, 'public-data-dir', compress)


def edxdatafs(namespace=False, compress=True):
    return _fslookup(namespace, 'edx-data-dir', compress)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
