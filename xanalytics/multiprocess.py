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

_pid = None
_qout = Queue(maxsize = 1000)
_processes = 0

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
        return i+1

    qin = Queue(maxsize=1000)

    _pid = forker()
    #print pid

    ## Feeder process
    if _pid == processes:
        for d in data:
            qin.put(d)
        for d in range(processes):
            qin.put(None)
        return []
    else:
        return iter(lambda : qin.get(), None)

def join(data):
    if _pid != 0:
        for d in data:
            _qout.put(d)
        _qout.put(None)
        sys.exit(0)
    
    iterables = [iter(lambda:_qout.get(), None) for x in range(_processes)]
    return itertools.chain(*iterables)

if __name__ == '__main__':
    data = split(range(100), 8)

    def compute(data):
        for d in data:
            time.sleep(1)
            yield d

    data = compute(data)
    data = join(data)
    print list(data)



# def f(qin,qout):
#     qout.put([42, None, 'hello'])

# 
#     qin = Queue()
#     qout = Queue()
#     qin.
#     p = Process(target=f, args=(qin, qout,))
#     p.start()
#     print qout.get()    # prints "[42, None, 'hello']"
#     p.join()
