x-analytics-scripts
===================

Assorted analytics scripts for edX tracking logs. These are my
personal scripts, and may not be useful to others.

In order to use these scripts, create a YAML file in your home
directory called ~/.xanalytics. This file should define several
directories:

 * public-data-dir     -- Non-edX datafiles. Geocoding. CIA World Factbook. Etc.
 * edx-data-dir        -- edX tracking logs and other read-only source data with PII
 * scratch-dir         -- Location for intermediate data. 

These scripts are based on processing log files with generators. This
is a nice design pattern for several reasons:

 * Fast. Most things are never written to disk. 
 * Easy-to-read. 
 * If you have a bug, scripts fail early. 
 * Things run lazily. This makes it easy to skip to whichever step
   where there is unprocessed data. 

See: http://www.slideshare.net/dabeaz/python-generator-hacking

I use pull (rather than push) generators, mostly for readability. It's
possible to go between the two with queues.

What the scripts provide
------------------------

Useful functions: 

 * xanalytics/settings.py -- Allows easy access to directories (as
   listed above) and similar. 