'''This is a script which will extract a snapshot of
courseware_studentmodule from the database. It makes no attempts at
consistency (so individual rows are correct, but some lines may be 10
hours out-of-date relative to others). This is intentional -- it makes
the whole thing workable. It goes through the database, 100,000 rows
at a time, and dumps them to files.

Once we've extracted, we can do nice processing on this.

It takes about 11 hours to run on all edX data as of Dec 2014. Annoying, but
doable. Threading would cut this down.

It is a one-off:

* The maximum number of lines is hardcoded, etc. COUNT(*) on csm is
slow, so the best way to get the maximum number of lines is select
MAX(id). The number in this file are NOT NECESSARILY a good estimate
for what is sensible for edx.org. I've used this for edx.org,
edge.edx.org, and random assorted servers.
* It is not threaded.
* Where files go is hardcoded.

Making this not one-off would involve:

* Explicit query for MAX(id), division, etc.
* Ideally, threading.
* Parameters for output, etc.
* Ideally, some way to combine/organize the output.
'''

import os
import datetime

start_time = datetime.datetime.now()
last_time = start_time

for i in range(4000):
    t = datetime.datetime.now()
    if i > 0:
        dt = t-last_time
        total_time = t-start_time
        progress = (4000-i)/float(i)  # items remaining divided by items done
        remaining_time = progress * total_time

        print i, "of 4000 time:", str(t),
        print "dt:", str(dt),
        print "ellapsed:", str(total_time),
        print "remaining", str(remaining_time)
    last_time = t
    minimum = i * 100000
    maximum = (i+1) * 100000+1
    filename = str(i)

    command = """echo '""" \
              """  select """ \
              """    module_id, student_id, grade, max_grade, course_id""" \
              """  from""" \
              """    courseware_studentmodule""" \
              """  where""" \
              """    id > {min} and id < {max} and module_type="problem" """ \
              """' | /opt/wwc/replica.sh |gzip > /mnt/extract/{file}.gz"""

    os.system(command.format(min=minimum, max=maximum, file=filename))
