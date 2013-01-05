import sys
import queue
import threading
import multiprocessing
import ctypes
from functools import wraps

import exlogging as log

def stop():
    for thread in threading.enumerate():
        if thread != threading.current_thread:
            thread.join()

@log.logged
class Thread:
    @log.ignore
    def __init__(self, limit=sys.maxsize, name=None):
        self.queue = queue.Queue()
        self.available = 0
        self.limit = limit
        self.workers = []
        self.name = name

    def post(self, func, *args, **kwargs):
        self.queue.put((func, args, kwargs, log.getcontext()))
        if self.available == 0 and len(self.workers) < self.limit:
            self.logger.debug('starting new worker. count={0}'.format(len(self.workers)))
            worker = threading.Thread(target=self.run, name=multiprocessing.current_process().name+'.'+self.name)
            worker.daemon = True
            self.workers.append(worker)
            worker.start()

    def stop(self):
        for worker in self.workers:
            self.queue.put(None)
        for worker in self.workers:
            worker.join()

    def run(self):
        libc = ctypes.CDLL('libc.so.6')
        tid = libc.syscall(186)
        with log.addcontext(tid):
            while True:
                self.available += 1
                item = self.queue.get()
                self.available -= 1
                if item is None:
                    break
                func, args, kwargs, context = item
                with log.addcontext(context):
                    try:
                        if func(*args, **kwargs):
                            break
                    except:
                        self.logger.error('exception while executing {0}({1}, {2})'.format(func, args, kwargs), exc_info=True)

def thread(*args, **kwargs):
    if len(args) == 1:
        func = args[0]
        kwargs['name'] = kwargs.get('name', func.__name__)
        worker = Thread(**kwargs)
        @wraps(func)
        def wrapper(*args, **kwargs):
            worker.post(func, *args, **kwargs)
        wrapper.worker = worker
        return wrapper
    else:
        return lambda func: thread(func, **kwargs)
