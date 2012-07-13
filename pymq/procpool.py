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

STOP = type('STOP', (object,), {})
WORKER_POLL_INTERVAL = 10

class Error(Exception): pass


class WorkerPool(object):
    """Multi-process worker pool

    :param broker_url: Broker URL.
    :param workers: Number of workers to maintain in the pool.
    :param get_task_timeout: Number of seconds to block while waiting for a new
        task to arrive on the queue. The default is 10 seconds. This timeout
        value affects pool stop time: a larger value means shutdown may take
        longer because it may need to wait longer for the consumer thread to
        complete.
    :param max_worker_tasks: Maximum number of tasks to execute on each worker
        before retiring the worker and spawning a new one in its place.
    """

    def __init__(self, broker_url, workers=None,
            get_task_timeout=10,
            max_worker_tasks=None,
        ):
        self.broker_url = broker_url
        if workers is None:
            try:
                workers = cpu_count()
            except NotImplementedError:
                workers = 1
        self.workers = workers
        self.get_task_timeout = get_task_timeout
        self.max_worker_tasks = max_worker_tasks
        self._workers = ThreadQueue()
        self._running = False

    def start(self, init_func, init_args=(), init_kwargs={}, handle_sigterm=1):
        """Start the worker pool

        :param init_func: Worker initializer. This is called to initialize each
            worker on startup. It can be used to setup logging or do any other
            global initialization. The first argument will be a broker instance,
            and the remaining will be `*init_args, **init_kwargs`.
        :param init_args: Additional arguments to pass to init_func.
        :param init_kwargs: Additional keyword arguments to pass to init_func.
        :param handle_sigterm: If True (the default) setup a signal handler
            and block until the process is signalled. This should only be
            called in the main thread in that case. If False, start workers and
            a pool manager thread and return.
        """
        if self._running:
            raise Error('cannot start already running WorkerPool')
        self._running = True
        log.info('starting worker pool...')

        for n in range(self.workers):
            worker = WorkerProxy(self.broker_url,
                init_func, init_args, init_kwargs, self.max_worker_tasks)
            self._workers.put(worker)

        self._consumer_thread = Thread(target=self._consume_tasks)
        self._consumer_thread.start()

        if handle_sigterm:
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
            # TODO handle error (e.g., queue went away)
            task = get_task(timeout)
            if task is None:
                put_worker(worker)
                continue
            worker.execute(task, put_worker)
        log.debug('consumer stopped')

    def stop(self):
        """Shutdown the pool

        This is probably only useful when the pool was started with
        `handle_sigterm=False`.
        """
        if not self._running:
            return False
        self._running = False

        log.info('shutting down...')
        drain_queue(self._workers, WorkerProxy.terminate)
        self._workers.put(STOP)
        self._consumer_thread.join()
        drain_queue(self._workers, WorkerProxy.terminate)
        log.info('worker pool stopped')
        return True


class WorkerProxy(object):

    def __init__(self, *args):
        self.pid = 'not-started'
        self.queue = ThreadQueue()
        self.thread = Thread(target=self._worker_loop, args=(args,))
        self.thread.start()

    def execute(self, task, return_to_pool):
        self.queue.put((task, return_to_pool))

    def terminate(self):
        self.queue.put((STOP, None))
        self.queue = None
        self.thread.join()

    def _worker_loop(self, args):
        proc = None
        queue = self.queue

        while True:
            if proc is None or not proc.is_alive():
                # start new worker process
                cn, cx = Pipe()
                proc = run_in_subprocess(worker_process, cx, *args)
                self.pid = proc.pid

            task, return_to_pool = queue.get()
            if task is STOP:
                try:
                    if proc is not None:
                        cn.send(STOP)
                        cn.close()
                        proc.join()
                except Exception:
                    log.error('%s failed to shutdown cleanly', self, exc_info=1)
                    raise
                finally:
                    log.debug('terminated %s', self)
                    self.pid = '%s-terminated' % self.pid
                break

            try:
                cn.send(task)
                while not cn.poll(WORKER_POLL_INTERVAL):
                    if not proc.is_alive():
                        raise Error('unknown cause of death')
                    # TODO update result heartbeat
                status = cn.recv()
            except Exception:
                log.error('%s died unexpectedly', self, exc_info=True)
                # NOTE could end up with zombie worker here if there was a
                # problem communicating but the worker did not actually die.
                # Is that even possible? It should terminate itself when
                # the pool is shutdown.
                proc = None
                cn.close()
            else:
                log.debug('%s completed task', self)
                if status is STOP:
                    proc = None
                    cn.close()
            finally:
                return_to_pool(self)

    def __str__(self):
        return 'WorkerProxy-%s' % self.pid


def worker_process(cn, url, init, init_args, init_kw, max_worker_tasks):
    broker = get_broker(url)
    init(broker, *init_args, **init_kw)
    log.info('Worker-%s initialized', os.getpid())
    task_count = 0
    while True:
        # TODO stop worker if parent dies
        task = cn.recv()
        if task is STOP:
            break

        broker.invoke(*task) # may never return (due to segfault in worker)

        task_count += 1
        if max_worker_tasks is None or task_count < max_worker_tasks:
            cn.send(None) # signal task completion
        else:
            cn.send(STOP) # signal task completion, worker stopping
            break
    log.info('Worker-%s stopped', os.getpid())


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
