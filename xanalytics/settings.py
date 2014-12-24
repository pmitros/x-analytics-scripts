import os.path
import yaml

import fs.osfs

from xanalytics.gzipfs import GZIPFS

settings_files = ['/etc/xanalytics', '~/.xanalytics']


class _settings(object):
    _settings = None

    def refresh(self):
        self._settings = dict()
        for f in settings_files:
            f = os.path.expanduser(f)
            if os.path.exists(f):
                self._settings.update(yaml.load(open(f)))

    def __getitem__(self, key):
        return self._settings[key]

    def __contains__(self, key):
        return key in self._settings

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
    return _fslookup(namespace, 'public-data-dir', compress)


def edxdatafs(namespace=False, compress=True):
    return _fslookup(namespace, 'edx-data-dir', compress)









##################
## Deprecations ##
##################

# def _filelookup(filename, namespace, directory, output):
#     '''
#     Deprecated: Helper for file lookups below.
#     '''
#     basepath = settings[directory]
#     if namespace:
#         path = os.path.join(basepath, namespace)
#         if not os.path.exists(path):
#             os.mkdir(path)
#     else:
#         path = basepath

#     filename = os.path.join(path, filename)
#     if not output and not os.path.exists(filename):
#         raise LookupError("No such file or directory {fn}".format(fn = filename))
#     return filename


# def edxdatafile(filename, namespace = False, output = False):
#     '''
#     Return the location of a datafile from the data file directory.

#     If output is False (default), raise an exception if file does not
#     exist.  Otherwise, return where the file ought to go.

#     DEPRECATED: USE DIRECTORY-LEVEL METHODS
#     '''
#     return _filelookup(filename, namespace, 'edx-data-dir', output)


# def publicdatafile(filename, namespace = False, output = False):
#     '''
#     Return the location of a datafile from the data file directory.

#     If output is False (default), raise an exception if file does not
#     exist.  Otherwise, return where the file ought to go.

#     DEPRECATED: USE DIRECTORY-LEVEL METHODS
#     '''
#     return _filelookup(filename, namespace, 'public-data-dir', output)


# def scratchfile(filename, namespace = False, output = True):
#     '''
#     Return the location of a datafile from the data file directory.

#     If output is False (default), raise an exception if file does not
#     exist.  Otherwise, return where the file ought to go.

#     DEPRECATED: USE DIRECTORY-LEVEL METHODS
#     '''
#     return _filelookup(filename, namespace, 'scratch-dir', output)


# def outputfile(filename, namespace = False, output = True):
#     '''
#     Return the location of a datafile from the data file directory.

#     If output is False (default true), raise an exception if file does not
#     exist.  Otherwise, return where the file ought to go.

#     DEPRECATED: USE DIRECTORY-LEVEL METHODS
#     '''
#     return _filelookup(filename, namespace, 'output-dir', output)



# datafile = publicdatafile  # DEPRECATED
