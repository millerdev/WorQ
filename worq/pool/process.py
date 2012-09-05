# WorQ - Python task queue
#
# Copyright (c) 2012 Daniel Miller
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Multi-process worker pool

Processes in the ``worq.pool.process`` stack:

* Queue - enqueues tasks to be executed
* Broker - task queue and results backend (redis)
* WorkerPool - worker pool manager process
* Worker - worker process, which does the real work
"""

import errno
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
from worq import get_broker
from worq.core import DAY, DEFAULT
from Queue import Empty, Queue as ThreadQueue
from threading import Thread

log = logging.getLogger(__name__)

STOP = 'STOP'
PYTHON_EXE = sys.executable
WORKER_POLL_INTERVAL = 30

class Error(Exception): pass


class WorkerPool(object):
    """Multi-process worker pool

    :param broker: Broker object.
    :param init_func: Worker initializer. This is called to initialize
        each worker on startup. It can be used to setup logging or do
        any other global initialization. The first argument will be a
        broker url, and the remaining will be ``*init_args,
        **init_kwargs``. It must return a broker instance, and it must
        be pickleable.
    :param init_args: Additional arguments to pass to init_func.
    :param init_kwargs: Additional keyword arguments to pass to init_func.
    :param workers: Number of workers to maintain in the pool. The default
        value is the number returned by ``multiprocessing.cpu_count``.
    :param max_worker_tasks: Maximum number of tasks to execute on each worker
        before retiring the worker and spawning a new one in its place.
    :param name: A name for this pool to distinguish its log output from that
        of other pools running in the same process.
    """

    def __init__(self, broker,
            init_func,
            init_args=(),
            init_kwargs=None,
            workers=None,
            max_worker_tasks=None,
            name=None,
        ):
        self.broker = broker
        self.init_func = init_func
        self.init_args = (broker.url,) + init_args
        self.init_kwargs = {} if init_kwargs is None else init_kwargs
        if workers is None:
            try:
                workers = cpu_count()
            except NotImplementedError:
                workers = 1
        self.workers = workers
        self.max_worker_tasks = max_worker_tasks
        if name is None and broker.name != DEFAULT:
            name = broker.name
        self.name = name
        self._workers = []
        self._worker_queue = ThreadQueue()
        self._running = False

    def __str__(self):
        parts = (type(self).__name__, os.getpid(), self.name)
        return '-'.join(str(p) for p in parts if p)

    def start(self, timeout=10, handle_sigterm=True):
        """Start the worker pool

        :param timeout: Number of seconds to block while waiting for a
            new task to arrive on the queue. This timeout value affects
            pool stop time: a larger value means shutdown may take
            longer because it may need to wait longer for the consumer
            thread to complete.
        :param handle_sigterm: If true (the default) setup a signal
            handler and block until the process is signalled. This
            should only be called in the main thread in that case. If
            false, start workers and a pool manager thread and return.
        """
        if handle_sigterm:
            return start_pools(self, timeout=timeout)
        if self._running:
            raise Error('cannot start already running WorkerPool')
        self._running = True

        for n in range(self.workers):
            worker = WorkerProxy(
                self.broker,
                self.init_func,
                self.init_args,
                self.init_kwargs,
                self.max_worker_tasks
            )
            self._workers.append(worker)
            self._worker_queue.put(worker)

        args = (timeout,)
        self._consumer_thread = Thread(target=self._consume_tasks, args=args)
        self._consumer_thread.start()
        log.info('%s started', str(self))

    def _consume_tasks(self, timeout):
        get_task = self.broker.next_task
        get_worker = self._worker_queue.get
        put_worker = self._worker_queue.put
        try:
            while True:
                worker = get_worker()
                if worker == STOP:
                    break
                task = get_task(timeout)
                if task is None:
                    put_worker(worker)
                    continue
                worker.execute(task, put_worker)
        except Exception:
            log.error('%s task consumer crashed', str(self), exc_info=True)
        log.debug('%s consumer stopped', str(self))

    def stop(self, join=True):
        """Shutdown the pool

        This is probably only useful when the pool was started with
        `handle_sigterm=False`.
        """
        if not self._running:
            return False
        self._running = False

        log.info('shutting down %s...', str(self))
        while True:
            try:
                item = self._worker_queue.get_nowait()
            except Empty:
                break
        self._worker_queue.put(STOP)
        for worker in self._workers:
            worker.stop()
        if join:
            self.join()
        return True

    def join(self):
        """Wait for pool to stop (call after ``.stop(join=False)``)"""
        assert not self._running, 'call stop() first'
        self._consumer_thread.join()
        self._worker_queue = ThreadQueue()
        for worker in self._workers:
            worker.join()
        log.info('%s stopped', str(self))


def start_pools(*pools, **start_kwargs):
    """Start one or more pools and wait indefinitely for SIGTERM or SIGINT

    This is a blocking call, and should be run in the main thread.
    """
    if not pools:
        raise ValueError('start_pools requires at least 1 argument, got 0')
    for pool in pools:
        pool.start(handle_sigterm=False, **start_kwargs)
    setup_exit_handler()
    log.info('Press CTRL+C or send SIGTERM to stop')
    try:
        # Sleep indefinitely waiting for a signal to initiate pool shutdown.
        while True:
            time.sleep(DAY)
    finally:
        for pool in pools:
            pool.stop(join=False)
        for pool in pools:
            pool.join()


class WorkerProxy(object):

    def __init__(self, *args):
        self.pid = 'not-started'
        self.queue = ThreadQueue()
        self.thread = Thread(target=self._proxy_loop, args=args)
        self.thread.start()

    def execute(self, task, return_to_pool):
        self.queue.put((task, return_to_pool))

    def stop(self):
        self.queue.put((STOP, None))

    def join(self):
        self.queue = None
        self.thread.join()

    def _proxy_loop(self, broker, *args):
        is_debug = partial(log.isEnabledFor, logging.DEBUG)
        pid = os.getpid()
        proc = None
        queue = self.queue

        def stop():
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

        while True:
            if proc is None or not proc.is_alive():
                # start new worker process
                child, parent = Pipe()
                cx = _reduce_connection(parent) # HACK reduce for pickle
                proc = run_in_subprocess(worker_process, pid, cx, *args)
                self.pid = proc.pid

            task, return_to_pool = queue.get()
            if task == STOP:
                stop()
                break

            try:
                child.send(task)
                while not child.poll(task.heartrate):
                    if not proc.is_alive():
                        broker.task_failed(task)
                        raise Error('unknown cause of death')
                    broker.heartbeat(task)
                (result, status) = child.recv()
                broker.set_result(task, result)
            except Exception:
                log.error('%s died unexpectedly', str(self), exc_info=True)
                child.close()
                proc.stdin.close()
                proc = None
            else:
                if is_debug():
                    log.debug('%s completed task', str(self))
                if status == STOP:
                    child.close()
                    proc.stdin.close()
                    proc = None
            finally:
                return_to_pool(self)

    def __str__(self):
        return 'WorkerProxy-%s' % self.pid


def worker_process(parent_pid, reduced_cn,
        init, init_args, init_kw, max_worker_tasks):
    broker = init(*init_args, **init_kw)
    log.info('Worker-%s started', os.getpid())
    task_count = 1
    parent = reduced_cn[0](*reduced_cn[1]) # HACK un-reduce connection

    while True:
        while not parent.poll(WORKER_POLL_INTERVAL):
            if os.getppid() != parent_pid:
                log.error('abort: parent process changed')
                return

        try:
            task = parent.recv()
        except EOFError:
            log.error('abort: parent fd closed')
            break
        if task == STOP:
            break

        result = broker.invoke(task, return_result=True)

        if max_worker_tasks is None:
            parent.send((result, None)) # send result
        elif task_count < max_worker_tasks:
            task_count += 1
            parent.send((result, None)) # send result
        else:
            parent.send((result, STOP)) # send result, worker stopping
            break

    log.info('Worker-%s stopped', os.getpid())


def run_in_subprocess(_func, *args, **kw):
    """Call function with arguments in subprocess

    All arguments to this function must be able to be pickled.

    Use ``subprocess.Popen`` rather than ``multiprocessing.Process``
    because we use threads, which do not play nicely with ``fork``. This
    was originally written with ``multiprocessing.Process``, which caused
    in intermittent deadlocks. See http://bugs.python.org/issue6721

    :returns: A ``PopenProcess`` object.
    """
    prog = 'from worq.pool.process import main; main()'
    # close_fds=True prevents intermittent deadlock in Popen
    # See http://bugs.python.org/issue2320
    proc = subprocess.Popen([PYTHON_EXE, '-c', prog],
                            stdin=subprocess.PIPE, close_fds=True,
                            preexec_fn=disable_signal_propagation)
    assert proc.stdout is None
    assert proc.stderr is None
    try:
        dump((_func, args, kw), proc.stdin, HIGHEST_PROTOCOL)
        proc.stdin.flush()
    except IOError as e:
        # copied from subprocess.Popen.communicate
        if e.errno != errno.EPIPE and e.errno != errno.EINVAL:
            proc.terminate()
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
    to be pickled. Connections pickled with this function should not be
    sent over an untrusted network.

    HACK work around ``multiprocessing`` connection authentication because
    we are using ``subprocess.Popen`` instead of ``multiprocessing.Process``
    to spawn new child processes.

    This will not be necessary when ``multiprocessing.Connection`` objects
    can be pickled. See http://bugs.python.org/issue4892
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
    """Make a ``subprocess.Popen`` object more like ``multiprocessing.Process``
    """

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


def disable_signal_propagation():
    # http://stackoverflow.com/a/5446983/10840
    os.setpgrp()


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
        signal.signal(signal.SIGINT, on_exit)
        signal.signal(signal.SIGTERM, on_exit)
