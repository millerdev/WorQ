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
import subprocess
from functools import partial
from multiprocessing import Pipe, cpu_count, current_process
from multiprocessing.process import AuthenticationString
from multiprocessing.reduction import reduce_connection, rebuild_connection
from cPickle import dump, load, HIGHEST_PROTOCOL, PicklingError
from pymq import get_broker
from pymq.core import DAY
from Queue import Empty, Queue as ThreadQueue
from threading import Thread

log = logging.getLogger(__name__)

STOP = 'STOP'
PYTHON_EXE = sys.executable
WORKER_POLL_INTERVAL = 30

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
        self._workers = []
        self._worker_queue = ThreadQueue()
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
            self._workers.append(worker)
            self._worker_queue.put(worker)

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
        get_worker = self._worker_queue.get
        put_worker = self._worker_queue.put
        while True:
            worker = get_worker()
            if worker == STOP:
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
        while True:
            try:
                item = self._worker_queue.get_nowait()
            except Empty:
                break
        self._worker_queue.put(STOP)
        self._consumer_thread.join()
        self._worker_queue = ThreadQueue()
        for worker in self._workers:
            worker.terminate()
        log.info('worker pool stopped')
        return True


class WorkerProxy(object):

    def __init__(self, *args):
        self.pid = 'not-started'
        self.queue = ThreadQueue()
        self.thread = Thread(target=self._proxy_loop, args=(args,))
        self.thread.start()

    def execute(self, task, return_to_pool):
        self.queue.put((task, return_to_pool))

    def terminate(self):
        self.queue.put((STOP, None))
        self.queue = None
        self.thread.join()

    def _proxy_loop(self, args):
        is_debug = partial(log.isEnabledFor, logging.DEBUG)
        pid = os.getpid()
        proc = None
        queue = self.queue

        while True:
            # check for STOP in queue ???
            if proc is None or not proc.is_alive():
                # start new worker process
                child, parent = Pipe()
                cx = _reduce_connection(parent) # HACK reduce for pickle
                proc = run_in_subprocess(worker_process, pid, cx, *args)
                self.pid = proc.pid

            task, return_to_pool = queue.get()
            if task == STOP:
                try:
                    if proc is not None:
                        child.send(STOP)
                        child.close()
                        proc.join()
                except Exception:
                    log.error('%s failed to stop cleanly',
                        str(self), exc_info=True)
                    raise
                else:
                    log.debug('terminated %s', str(self))
                finally:
                    self.pid = '%s-terminated' % self.pid
                break

            try:
                child.send(task)
                while not child.poll(WORKER_POLL_INTERVAL):
                    if not proc.is_alive():
                        raise Error('unknown cause of death')
                    # TODO update result heartbeat
                status = child.recv()
            except Exception:
                log.error('%s died unexpectedly', self, exc_info=True)
                # NOTE could end up with zombie worker here if there was a
                # problem communicating but the worker did not actually die.
                # Is that even possible? It should terminate itself when
                # the pool is shutdown.
                proc = None
                child.close()
            else:
                if is_debug():
                    log.debug('%s completed task', str(self))
                if status == STOP:
                    proc = None
                    child.close()
            finally:
                return_to_pool(self)

    def __str__(self):
        return 'WorkerProxy-%s' % self.pid


def worker_process(parent_pid, reduced_cn, url,
        init, init_args, init_kw, max_worker_tasks):
    broker = get_broker(url)
    init(broker, *init_args, **init_kw)
    log.info('Worker-%s started', os.getpid())
    task_count = 1
    parent = reduced_cn[0](*reduced_cn[1]) # HACK un-reduce connection
    while True:
        while not parent.poll(WORKER_POLL_INTERVAL):
            if os.getppid() != parent_pid:
                log.error('abort: parent process changed')
                return

        task = parent.recv()
        if task == STOP:
            break

        broker.invoke(*task)

        if max_worker_tasks is None:
            parent.send(None) # signal task completion
        elif task_count < max_worker_tasks:
            task_count += 1
            parent.send(None) # signal task completion
        else:
            parent.send(STOP) # signal task completion, worker stopping
            break

    log.info('Worker-%s stopped', os.getpid())


def run_in_subprocess(_func, *args, **kw):
    """Call function with arguments in subprocess

    All arguments to this function must be able to be pickled.

    Use subprocess.Popen rather than multiprocessing.Process because we use
    threads, which do not play nicely with fork. This was originally written
    with multiprocessing.Process, which caused in intermittent deadlocks.
    See http://bugs.python.org/issue6721

    :returns: A subprocess.Popen object.
    """
    prog = 'from pymq.procpool import main; main()'
    proc = subprocess.Popen([PYTHON_EXE, '-c', prog], stdin=subprocess.PIPE)
    assert proc.stdout is None
    assert proc.stderr is None
    try:
        dump((_func, args, kw), proc.stdin, HIGHEST_PROTOCOL)
        proc.stdin.flush()
    except IOError as e:
        # copied from subprocess.Popen.communicate
        if e.errno != errno.EPIPE and e.errno != errno.EINVAL:
            raise
    except PicklingError:
        proc.terminate()
        raise
    return PopenProcess(proc)


def main():
    func, args, kw = load(sys.stdin)
    try:
        func(*args, **kw)
    except Exception:
        log.critical('subprocess crashed', exc_info=True)
        raise


def _reduce_connection(conn):
    """Reduce a connection object so it can be pickled.

    WARNING this puts the current process' authentication key in the data
    to be pickled. Connections pickled with this connection should not be
    sent over a network.

    HACK work around multiprocessing connection authentication because
    we are using subprocess.Popen instead of multiprocessing.Process to
    spawn new child processes.

    This will not be necessary when multiprocessing.Connection objects can be
    pickled. See http://bugs.python.org/issue4892
    """
    obj = reduce_connection(conn)
    assert obj[0] is rebuild_connection, obj
    assert len(obj) == 2, obj
    args = (bytes(current_process().authkey),) + obj[1]
    return (_rebuild_connection, args)

def _rebuild_connection(authkey, *args):
    current_process().authkey = AuthenticationString(authkey)
    return rebuild_connection(*args)


class PopenProcess(object):
    """Make a subprocess.Popen object more like multiprocessing.Process"""

    def __init__(self, proc):
        self._proc = proc

    def is_alive(self):
        return self._proc.poll() is None

    def join(self, timeout=None):
        if timeout is None:
            self._proc.communicate()
            return
        end = time.time() + timeout
        while time.time() < end:
            if not self.is_alive():
                return
            time.sleep(0.01)

    def __getattr__(self, name):
        return getattr(self._proc, name)


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
