import os.path
import yaml

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

def datafile(filename, output = False):
    '''
    Return the location of a datafile from the data file directory. 

    If output is False (default), raise an exception if file does not
    exist.  Otherwise, return where the file ought to go.
    '''
    filename = os.path.join(settings['data-dir'], filename)
    if not output and not os.path.exists(filename):
        raise LookupError("No such file or directory {fn}".format(fn = filename))
    return filename
