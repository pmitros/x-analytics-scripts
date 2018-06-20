'''
This is a module which allows us to use multicore and
multiprocessor machines to efficiently process data. split() will
split operations among multiple CPUs, while join() will recombine
them. This is complete transparent for mapping and filtering
operations.

Caveats:

* This will not work for general accumulation-type operations. It is
  possible to phrase some types of accumulations as a
  split()/accumulate/join()/accumulate if the operations are
  commutative and associative.
* This code was designed to be used once in a program (the queues are
  global variables). It'd be easy enough to fold these in either by
  making this into an object or returing two return values, but the
  API signatures would be a little more messy. Probably worth doing at
  some point.
* The data comes back a little out-of-order. This is by design. We
  could tag data such that it came back in-order, but it'd add
  performance overhead, and prevent some types of accumulation use cases.
'''

import itertools
import os
import sentinel
import sys
import time

from multiprocessing import Queue


_pid = None
_qout = Queue(maxsize=1000)
_processes = 0

# EndOfQueue is a hack. We could add metadata, but with a slight
# bit of overhead -- the multiprocess queues can't take all data
# types, so we'd need a wrapper. ('multiprocessing.Queue' didn't
# give a reliable way of detecting when we were done otherwise --
# we should check if that has been addressed since).
EndOfQueue = "End of queue"
FeederThread = sentinel.create('FeederThread')
ConsumerThread = sentinel.create('ConsumerThread')


def _log(task, item):
    '''
    Helper for debugging.
    '''
    if False:
        print "[{pid}] [{task}]: {item}".format(pid=_pid,
                                                task=task,
                                                item=item)


def split(data, processes):
    '''
    Fork off a number of processes. The data processing will be split
    among those processes.

    The 'data' goes into a queue. Processes pick of data from the
    queue until the queue is exhausted.

    'processes' is the number of processes to start.
    '''
    global _pid, _qout, _processes
    _processes = processes

    def forker():
        '''
        Fork `processes` processes. Return the process number
        for workers. Return the sentinel FeederThread for the
        source, and ConsumerThread for the sink.

                       /-> worker 1 --\
                       |              |
        FeederThread --+-> worker 2 --+--> ConsumerThread
                       |              |
                             ...
                       |              |
                       \-> worker n --/
        '''
        for i in range(processes):
            process = os.fork()
            if process != 0:
                return i
        if os.fork() == 0:
            return FeederThread
        else:
            return ConsumerThread

    qin = Queue(maxsize=1000)

    _pid = forker()

    if _pid == FeederThread:
        # The feeder process just takes data and puts it
        # into the queue, followed by an EndOfQueue. It
        # then closes the queue and terminates.
        for d in data:
            qin.put(d, block=True)
        for d in range(processes):
            qin.put(EndOfQueue, block=True)
        qin.close()
        qin.join_thread()
        return []
    elif _pid == ConsumerThread:
        # The consumer thread shouldn't do any processing -- so it
        # gets no items
        return []
    else:
        # All the worker threads just pull from the queue
        return iter(lambda: qin.get(block=True), EndOfQueue)


def join(data):
    '''
    Merge data from all worker processes into the main process.
    '''
    def pull_data():
        d = _qout.get(block=True)
        _log("PULL", d)
        return d

    # If we're in the consumer thread, pull all the data out.
    if _pid == ConsumerThread:
        iterables = [iter(pull_data, EndOfQueue) for x in range(_processes+1)]
        return itertools.chain(*iterables)

    # If we're in the worker threads or the source thread, and we're done,
    # clean up and exit.
    for d in data:
        _log("PUT", d)
        _qout.put(d, block=True)
    _log("PUT", "EOQ")
    _qout.put(EndOfQueue, block=True)
    _qout.close()
    _qout.join_thread()
    os._exit(0)
    return []


# If we run directly, we do a quick test. We split 100 1 second sleeps among
# 8 processes. If we're working fine, this will take 12 seconds.
#
# We should print 100, followed by numbers from 0 to 49.5 with a step of 0.5
# slightly out-of-order
if __name__ == '__main__':
    data = split(range(100), 8)

    def compute(data):
        for d in data:
            time.sleep(1)
            yield d/2.

    data = compute(data)
    data = join(data)
    data = list(data)
    print len(data)
    print data
