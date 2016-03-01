x-analytics-scripts
===================

Assorted analytics scripts for edX tracking logs. These are my
personal scripts, and may not be useful to others.

In order to use these scripts, create a YAML file in your home
directory called `~/.xanalytics`. This file should define several
directories:

- `public-data-dir`     -- Non-edX datafiles. Geocoding. CIA World Factbook. Etc.
- `edx-data-dir`        -- edX tracking logs and other read-only source data with PII
- `scratch-dir`         -- Location for intermediate data. 

These directories have subdirectories. Each subdirectory will contain
either subdirectories, or gzip-compressed files.

These scripts are based on processing log files with generators. This
is a nice design pattern for several reasons:

- Fast. Most things are never written to disk. 
- Easy-to-read. 
- If you have a bug, scripts fail early. 
- Things run lazily. This makes it easy to skip to whichever step
   where there is unprocessed data. 

See: http://www.slideshare.net/dabeaz/python-generator-hacking

I use pull (rather than push) generators, mostly for readability. It's
possible to go between the two with queues.

The scripts can process data for two courses in about 3 minutes on a
quad core i7 machine.

Useful things
------------------------

This is likely very out-of-date by the time you read this. 

`xanalytics/settings.py` -- Allows easy access to directories (as
listed above) and similar. We're moving to pyfilesystem, but many
functions still take directories. pyfilesystem is a higher-level
abstraction, and is easier to work with (less `os.join` and
similar). In the future, it can also transparently go to/from S3.

`xanalytics/cia.py` -- CIA world factbook access. Neat statistics about
student countries.

`xanalytics/multiprocess.py` -- Allows you to split computation among
multiple cores. There's a bit of overhead for serializing data, so
it's not always a win, but it is almost always a win. `split` and
`join` are the functions to look at. The cool thing is this is
completely transparent. Most of the code doesn't have to be aware of
these.

`xanalytics/streaming.py` -- Most of the meat of the code. Various
processing operations over tracking logs. `read_data` is usually the
starting point, followed by something like `text_to_json` if using
source files. In most cases, I suggest using BSON files. It's a 4x
performance gain. `encode_to_bson` is helpful here. We do need to move
`read_bson_file` to correctly work with the filesystem (rather than
directory) approach.

`xanalytics/desensitize.py` -- Make it easier to work with PII without
unintentionally violating student privacy.
