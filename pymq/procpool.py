"""Multi-process worker pool implementation

Processes in the pymq.procpool stack:
    Producer - produces tasks to be executed
    Queue - message queue and results backend (redis)
    Broker - worker pool manager
    Worker - does the real work
"""

import logging
import os
import sys
import time
from multiprocessing import Pool, Process, Pipe, cpu_count
from pymq import get_broker
from pymq.core import DAY
from Queue import Empty, Queue as ThreadQueue
from threading import Thread

log = logging.getLogger(__name__)

GET_TASK_TIMEOUT = 10
WORKER_POLL_INTERVAL = 5
STOP = type('STOP', (object,), {})


class WorkerPool(object):

    def __init__(self, broker_url,
            num_workers=None,
            get_task_timeout=GET_TASK_TIMEOUT,
            handle_sigterm=True):
        self.broker_url = broker_url
        self.get_task_timeout = get_task_timeout
        if num_workers is None:
            try:
                num_workers = cpu_count()
            except NotImplementedError:
                num_workers = 1
        self.num_workers = num_workers
        if handle_sigterm:
            setup_exit_handler()

    def start(_self, _init, *args, **kw):
        self = _self
        url = self.broker_url
        workers = ThreadQueue()
#        returns = ThreadQueue()
        for n in range(self.num_workers):
            workers.put(Worker(url, _init, args, kw))

        def consumer_thread():
            broker = get_broker(url)
            timeout = self.get_task_timeout
            get_task = broker.messages.get

            def make_return_worker_callback(worker):
                def return_worker():
                    log.debug('returning %s to pool', worker)
                    workers.put(worker)
                return return_worker

            while True:
                worker = workers.get()
                if worker is STOP:
                    log.debug('stopping consumer')
                    return
                task = get_task(timeout)
                if task is None:
                    workers.put(worker)
                    continue
                log.debug('sending task to worker')
                worker.execute(task, make_return_worker_callback(worker))

#        def monitor_thread():
#            while True:
#                worker = returns.get()
#                if worker is STOP:
#                    log.debug('stopping monitor')
#                    break
#                workers.put(worker)

        consumer = Thread(target=consumer_thread)
        consumer.start()
#        monitor = Thread(target=monitor_thread)
#        monitor.start()

        try:
            # HACK how else to sleep indefinitely? The main thread receives
            # signals, which should not interrupt anything critical.
            while True:
                time.sleep(DAY)
        finally:
            log.info('shutting down...')
            def term(worker):
                worker.terminate()
                log.debug('terminated %s', worker)
            empty_queue(workers, term)
            #empty_queue(returns, term)
            workers.put(STOP)
            #returns.put(STOP)
            consumer.join()
            #monitor.join()
            empty_queue(workers, term)
            #empty_queue(returns, term)


class Worker(object):

    def __init__(self, *args):
        self.args = args
        self.pipe, pipe = Pipe()
        self.proc = run_in_subprocess(worker_process, pipe, *args)

    def __str__(self):
        return 'Worker-%s' % self.proc.pid

    def execute(self, task, callback):
        def run():
            self.pipe.send(task)
            # TODO update result heartbeat
            # TODO handle worker died
            self.pipe.recv()
            callback()
        Thread(target=run).start()

    def terminate(self):
        self.pipe.send(STOP)
        self.proc.join()


def worker_process(pipe, url, init, init_args, init_kw):
    broker = get_broker(url)
    init(broker, *init_args, **init_kw)
    log.debug('started Worker-%s', os.getpid())
    while True:
        # TODO stop worker if parent dies
        task = pipe.recv()
        if task is STOP:
            break
        broker.invoke(*task)
        pipe.send(None)


def empty_queue(queue, handle_item):
    while True:
        try:
            item = queue.get_nowait()
        except Empty:
            break
        handle_item(item)


def setup_exit_handler():
    # http://danielkaes.wordpress.com/2009/06/04/how-to-catch-kill-events-with-python/
    def on_exit(sig, func=None):
        sys.exit()
    if os.name == "nt":
        try:
            import win32api
            win32api.SetConsoleCtrlHandler(on_exit, True)
        except ImportError:
            version = ".".join(map(str, sys.version_info[:2]))
            raise Exception("pywin32 not installed for Python " + version)
    else:
        import signal
        signal.signal(signal.SIGTERM, on_exit)


def run_in_subprocess(_func, *args, **kw):
    proc = Process(target=exclogger, args=(_func, args, kw))
    proc.start()
    return proc

def exclogger(func, args, kw):
    try:
        func(*args, **kw)
    except Exception:
        log.critical('crash!', exc_info=True)
        raise
