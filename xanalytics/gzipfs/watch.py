"""
xanalytics.gzipfs.watch
=============

Change watcher support for GZIPFS

"""

import os
import sys
import errno
import threading

from fs.errors import *
from fs.path import *
from fs.watch import *

GZIPFSWatchMixin = None

#  Try using native implementation on win32
if sys.platform == "win32":
    try:
        from xanalytics.gzipfs.watch_win32 import GZIPFSWatchMixin
    except ImportError:
        pass

#  Try using pyinotify if available
if GZIPFSWatchMixin is None:
    try:
        from xanalytics.gzipfs.watch_inotify import GZIPFSWatchMixin
    except ImportError:
        pass

#  Fall back to raising UnsupportedError
if GZIPFSWatchMixin is None:
    class GZIPFSWatchMixin(object):
        def __init__(self, *args, **kwargs):
            super(GZIPFSWatchMixin, self).__init__(*args, **kwargs)
        def add_watcher(self,*args,**kwds):
            raise UnsupportedError
        def del_watcher(self,watcher_or_callback):
            raise UnsupportedError


