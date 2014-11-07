'''
This is a module which allows us to use multicore and
multiprocessor machines to efficiently process data. split() will
split operations among multiple CPUs, while join() will recombine
them. This is complete transparent for mapping and filtering
operations. However, the data will come back a little out-of-order.

This will not work for accumulation-type operations, in most
cases. Although it would be possible to phrase some types of
accumulations as a split()/accumulate/join()/accumulate.
'''

from multiprocessing import Queue
import os, sys, itertools, time

import sentinel

_pid = None
_qout = Queue(maxsize = 1000)
_processes = 0

EndOfQueue = "End of queue"
FeederThread = sentinel.create('FeederThread')
ConsumerThread = sentinel.create('ConsumerThread')

def log(task, item):
    if False:
        print "[{pid}] [{task}]: {item}".format(pid = _pid, task = task, item = item)

def split(data, processes):
    '''
    Fork off a number of processes. The data processing will be split among those processes.

    The data goes into a queue. Processes pick of data from the queue until the queue is exhausted.

    processes is the number of processes to start.
    '''
    global _pid, _qout, _processes
    _processes = processes
    def forker():
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
    #print "pid", _pid

    ## Feeder process
    if _pid == FeederThread:
        for d in data:
            qin.put(d, block=True)
        for d in range(processes):
            qin.put(EndOfQueue, block=True)
        qin.close()
        qin.join_thread()
        return []
    elif _pid == ConsumerThread:
        return []
    else:
        return iter(lambda : qin.get(block=True), EndOfQueue)

def join(data):
    def pull_data():
        #print "Attempting PULL"
        d = _qout.get(block=True)
        log("PULL", d)
        return d

    if _pid == ConsumerThread:
        #print "Iterables"
        iterables = [iter(pull_data, EndOfQueue) for x in range(_processes+1)]
        #print "Done iterables"
        return itertools.chain(*iterables)

    for d in data:
        log("PUT", d)
        _qout.put(d, block=True)
    log("PUT", "EOQ")
    _qout.put(EndOfQueue, block=True)
    _qout.close()
    _qout.join_thread()
    os._exit(0)
    return []

if __name__ == '__main__':
    data = split(range(100), 8)

    def compute(data):
        for d in data:
            time.sleep(1)
            yield d/2.

    data = compute(data)
    data = join(data)
    data= list(data)
    print len(data)
    print data
