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
            workers=None,
            get_task_timeout=GET_TASK_TIMEOUT,
            handle_sigterm=True):
        self.broker_url = broker_url
        if workers is None:
            try:
                workers = cpu_count()
            except NotImplementedError:
                workers = 1
        self.workers = workers
        self.get_task_timeout = get_task_timeout
        self.handle_sigterm = handle_sigterm
        self._workers = ThreadQueue()
        self._running = False

    def start(_self, _init, *args, **kw):
        self = _self
        self._running = True
        for n in range(self.workers):
            self._workers.put(Worker(self.broker_url, _init, args, kw))

        self._consumer_thread = Thread(target=self._consume_tasks)
        self._consumer_thread.start()

        if self.handle_sigterm:
            setup_exit_handler()
            try:
                # HACK how else to sleep indefinitely? The main thread receives
                # signals, which should not interrupt anything critical, so its
                # sole purpose is to be the signal handler.
                while True:
                    time.sleep(DAY)
            finally:
                self.stop()

    def _consume_tasks(self):
        timeout = self.get_task_timeout
        broker = get_broker(self.broker_url)
        get_task = broker.messages.get
        get_worker = self._workers.get
        put_worker = self._workers.put
        while True:
            worker = get_worker()
            if worker is STOP:
                break
            task = get_task(timeout)
            if task is None:
                put_worker(worker)
                continue
            worker.execute(task, put_worker)
        log.debug('consumer stopped')

    def stop(self):
        if not self._running:
            return False
        self._running = False

        log.info('shutting down...')
        drain_queue(self._workers, Worker.terminate)
        self._workers.put(STOP)
        self._consumer_thread.join()
        drain_queue(self._workers, Worker.terminate)
        return True


class Worker(object):

    def __init__(self, *args):
        self.args = args
        self.pipe, pipe = Pipe()
        self.proc = run_in_subprocess(worker_process, pipe, *args)

    def __str__(self):
        return 'Worker-%s' % self.proc.pid

    def execute(self, task, return_to_pool):
        def run():
            log.debug('sending task to worker')
            self.pipe.send(task)

            # TODO update result heartbeat
            # TODO handle worker died
            self.pipe.recv()

            log.debug('returning %s to pool', self)
            return_to_pool(self)
        Thread(target=run).start()

    def terminate(self):
        self.pipe.send(STOP)
        self.proc.join()
        log.debug('terminated %s', self)


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


def drain_queue(queue, handle_item):
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
