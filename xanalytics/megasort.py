'''This is a system designed to sort pretty big files. It will:

* Break up the file into little pieces, and sort those in
  memory
* Make successive runs of merge sort to rejoin into one
  file

This will not run out of file pointers, memory, or have similar issues
often associated with pretty big data. However, it's not parallel, or
distributed, so it will not scale to very big files.

It's not a bad starting point for a cluster sort like that. The steps
would be:
* Distribute the initial sort across the cluster
* Have machines on the cluster do the merge pass for groups of files,
  but merge into several files, with fixed break points
* Have machines do a final merge on those files

It would make sense to do this just to use multiple cores on one
machine. A naive version of this -- where I queued in objects into
individual forks -- was slower than not doing this at all. The queuing
took longer than the first-pass sorting. A non-naive, blocked version
ought to give an 8x performance boost, however.

Progress indication would be kind of nice too.

Right now, the key is NOT used correctly. This is a TODO.
'''

import fs.tempfs
import heapq
import itertools
import md5
import sys
import time
import uuid

import xanalytics.multiprocess

from xanalytics.streaming import snoop


def filename_generator():
    '''
    Generate a random, unique filename for a temporary file

    This is a test case which creates two generators, each of which
    generates two filenames. We then test as a Cartesian product
    that all are unique and 38 characrers long.

    >>> import itertools
    >>> fg1 = filename_generator()
    >>> f11 = fg1.next()
    >>> f12 = fg1.next()
    >>> fg2 = filename_generator()
    >>> f21 = fg2.next()
    >>> f22 = fg2.next()
    >>> fns = [f11,f12,f21,f22]
    >>> [a==b for a,b in itertools.product(fns, fns)]
    [True, False, False, False,\
 False, True, False, False,\
 False, False, True, False,\
 False, False, False, True]
    >>> len(f11), len(f12), len(f21), len(f22)
    (38, 38, 38, 38)

    '''
    # First, we make a random string as a base for the file.
    # This guarantees pretty it is pretty unique.
    m = md5.new()
    for s in [uuid.uuid1(), uuid.uuid4()]:
        m.update(str(s))
    base = m.hexdigest()
    i = 0
    while True:
        yield "tmp-{i}-{base}".format(i=i, base=base)
        i = i+1


def file_generator(filesystem):
    '''
    Returns an iterator of temporary files. Returns pairs of filenames
    and file pointers to write to those files.
    '''
    fg = filename_generator()
    for filename in fg:
        yield (filename, filesystem.open(filename, "w"))


def quick_sort_sets(data_source,
                    filesystem=fs.tempfs.TempFS(),
                    key=None,
                    cmp=cmp,
                    file_limit=1000,
                    ramsize=1000,
                    breakpoints=[],
                    ram_requirements=lambda x: 1):
    """
    On a first pass, we break up the input into many small, sorted
    files. This is a helper which makes that first pass.
    """
    items = []
    total_size = [0]  # Hack for Python 2.x scoping rules. See:
    # http://stackoverflow.com/questions/4851463/python-closure-write-to-variable-in-parent-scope

    if not key:
        def key(x):
            x

    fg = file_generator(filesystem)

    def init():
        del items[:]
        total_size[0] = 0

    def finalize():
        items.sort(key=lambda x: x[0], cmp=cmp)
        (filename, filepointer) = fg.next()
        filepointer.writelines(unicode("\t".join(line)+'\n') for line in items)
        filepointer.close()
        return filename

    for item in data_source:
        items.append((key(item), item.strip()))
        total_size[0] += ram_requirements(item)
        if total_size[0] > ramsize:
            yield finalize()
            init()
    yield finalize()


def megasort(data_source,
             filesystem=fs.tempfs.TempFS(),
             key=None,
             cmp=cmp,
             file_limit=1000,
             ramsize=1000,
             breakpoints=[],
             ram_requirements=lambda x: 1):
    """
    This allows us to sort big-ish data. We need a key to sort on.

    Algorithm:

    Step through data. Pick out blocks of size `ramsize`. Sort them. Write
    them to disk. If we want to be more nuanced, we can assign a size

    Open up `file_limit` files. Merge them. Delete the originals.

    Iterate.

    TODO: We write out bad blocks to an additional file, `err`
    """
    files = []

    start_time = time.time()

    # First, we make a bunch of small, internally-sorted files. Each
    # line has a key, tab, then line.
    #
    # To make parallel:
    # Change data_source to xanalytics.multiprocess.split(data_source, 16)
    file_iter = quick_sort_sets(data_source,
                                filesystem,
                                key,
                                cmp,
                                file_limit,
                                ramsize,
                                breakpoints,
                                ram_requirements)

    remaining_files = itertools.chain(file_iter, files)

    fg = file_generator(filesystem)

    # Now, we merge these files.
    while True:
        # We grab the first bunch of files
        working_files = list(itertools.islice(remaining_files, file_limit))
        if len(working_files) < 2:
            break
        # We grab pointers to data from those files
        working_filepointers = ((x.split('\t') for x in filesystem.open(fn))
                                for fn in working_files)
        # We merge them
        output = heapq.merge(*working_filepointers)
        # We make a new file, and dump the merged results there
        (filename, filepointer) = fg.next()
        filepointer.writelines(unicode("\t".join(line)) for line in output)
        filepointer.close()
        # And we update out file list with the new file, removing the old ones
        files.append(filename)
        for file in working_files:
            filesystem.remove(file)
        print >> sys.stderr, "Sort pass", len(files), time.time()-start_time

    # We might end up with the final file in working_files or not,
    # depending on how things line up with the islice above. If we run
    # out of items in working_files, then hit the end of the
    # generator, and then add to files, the generator will be done,
    # and we'll end up with the filename only in files. If we run out
    # of files but don't hit the end of the generator, we'll end up
    # with this in working files and files. It stays in files either
    # way (for now) since that's an array.
    print >> sys.stderr, "Working:", working_files
    print >> sys.stderr, "Files:", files
    print >> sys.stderr, "Combined:", working_files+files
    print >> sys.stderr, filesystem.listdir()
    out = filesystem.open(files[-1])
    for line in out:
        try:
            line = line.split('\t')[1]
        except:
            print >> sys.stderr, "[", line, "]"
            raise
        yield line.strip()

if __name__ == '__main__':
    '''
    This is a simple test case which will do four rounds of merge sort
    in-memory. It confirms there are 10k items at the end, and that
    they are in-order.
    '''
    import doctest
    doctest.testmod()

    # Test case, we do a small number of items and a large number
    # of splits.
    ITEM_COUNT = 1e4
    FILE_LIMIT = 50
    RAM_SIZE = 50

    bench = False

    # Benchmark, we do a large number of items, and a realistic
    # number of splits
    if bench:
        # Baseline: Unthreaded was 50 seconds.
        # 43 seconds in initial sorts
        # 5 seconds in merge pass
        ITEM_COUNT = 2.9e6
        FILE_LIMIT = 5000
        RAM_SIZE = 5000

        # Smaller data size:
        # Baseline was 17 seconds
        # Using all 8 cores brought this up to 33 second, ironically
        # TempFS vs. in-memory made no big difference
        # ITEM_COUNT = 1e6
        # FILE_LIMIT = 1000
        # RAM_SIZE = 1000

    import fs.memoryfs
    filesystem = fs.memoryfs.MemoryFS()

    def data_generator():
        '''
        Generate a 10k lines for a-sortin'
        '''
        for i in range(int(ITEM_COUNT)):
            yield unicode(uuid.uuid4())

    data = data_generator()
    new_data = megasort(data,
                        filesystem,
                        file_limit=FILE_LIMIT,
                        ramsize=RAM_SIZE,
                        # key=lambda x:"".join(reversed(x)),
                        breakpoints="1234567890abcdef")

    old_item = None
    c = []
    d = []
    for item in new_data:
        if old_item:
            c.append(item > old_item)
            d.append(item == old_item)
        old_item = item
    if len(c)+1 != ITEM_COUNT:
        raise "We did not have the correct number of items"
    if not reduce((lambda x, y: x and y), c):
        raise "Items were not in the correct order"
    if reduce((lambda x, y: x or y), d):
        raise "Possible duplicate item"
