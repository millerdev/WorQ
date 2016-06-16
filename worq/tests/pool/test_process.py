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

import os
import sys
import signal
import logging
import logging.config

from contextlib import contextmanager
from nose.tools import nottest
from os.path import dirname, exists, join
from worq import get_broker, get_queue
from worq.pool.process import WorkerPool, Error, run_in_subprocess
from worq.task import Task, TaskExpired
from worq.tests.util import (assert_raises, eq_, eventually, tempdir,
    with_urls, WAIT)

log = logging.getLogger(__name__)


@with_urls
def test_WorkerPool_start_twice(url):
    broker = get_broker(url)
    pool = WorkerPool(broker, get_broker, workers=1)
    with start_pool(pool):
        with assert_raises(Error):
            pool.start(handle_sigterm=False)


@with_urls
def test_WorkerPool_max_worker_tasks(url):
    broker = get_broker(url)
    pool = WorkerPool(broker, WorkerPool_max_worker_tasks_init,
        workers=1, max_worker_tasks=3)
    with start_pool(pool):

        q = get_queue(url)
        res = q.results([q.func() for n in range(4)])
        assert res.wait(WAIT), repr(res)

        results = res.value
        assert isinstance(results, list), results
        eq_([r[1] for r in results], [1, 2, 3, 1])
        eq_(len(set(r[0] for r in results)), 2)


def WorkerPool_max_worker_tasks_init(url):
    broker = get_broker(url)
    calls = [0]

    @broker.expose
    def func():
        calls[0] += 1
        return (os.getpid(), calls[0])

    @broker.expose
    def results(res):
        return res

    return broker


@with_urls
def test_WorkerPool_heartrate(url):
    broker = get_broker(url)
    pool = WorkerPool(broker, WorkerPool_heartrate_init, workers=1)
    with start_pool(pool):

        q = get_queue(url)

        res = Task(q.suicide_worker, heartrate=0.1, result_timeout=5)()
        assert res.wait(WAIT), repr(res)
        print(repr(res))
        with assert_raises(TaskExpired):
            res.value


def WorkerPool_heartrate_init(url):
    broker = get_broker(url)

    @broker.expose
    def suicide_worker():
        log.warn("it's nice to work alone")
        os.kill(os.getpid(), signal.SIGKILL)  # force kill worker

    broker.expose(os.getpid)

    return broker


@nottest  # this is a very slow test, and doesn't seem that important
@with_urls(exclude='memory')
def test_WorkerPool_crashed_worker(url):
    broker = get_broker(url)
    pool = WorkerPool(broker, WorkerPool_crashed_worker_init, workers=1)
    with start_pool(pool):

        q = get_queue(url)

        res = q.getpid()
        assert res.wait(WAIT), repr(res)
        pid = res.value

        q.kill_worker()

        res = q.getpid()
        assert res.wait(WAIT), repr(res)
        assert res.value != pid, pid


def WorkerPool_crashed_worker_init(url):
    broker = get_broker(url)

    @broker.expose
    def kill_worker():
        log.warn('alone we crash')
        sys.exit()

    broker.expose(os.getpid)

    return broker


@with_urls(exclude='memory')
def test_WorkerPool_sigterm(url):
    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url,
            WorkerPool_sigterm_init, (tmp, logpath), workers=3)

        with printlog(logpath), force_kill_on_exit(proc):

            q = get_queue(url)

            q.func('text')

            eventually(reader(tmp, 'func.started'), '')

            proc.terminate()  # signal pool shutdown
            touch(join(tmp, 'func.unlock'))  # allow func to proceed

            eventually(reader(tmp, 'func.out'), 'text')
            eventually(verify_shutdown(proc), True, timeout=WAIT)


def WorkerPool_sigterm_init(url, tmp, logpath):
    process_config(logpath, 'Worker-%s' % os.getpid())
    broker = get_broker(url)

    @broker.expose
    def func(arg, lock=None):
        # signal func started
        touch(join(tmp, 'func.started'))

        # wait for unlock
        eventually(reader(tmp, 'func.unlock'), '')

        # write data to file
        touch(join(tmp, 'func.out'), arg)

        log.debug('func complete')

    return broker


@with_urls(exclude='memory')
def test_WorkerPool_worker_shutdown_on_parent_die(url):
    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url,
            WorkerPool_worker_shutdown_on_parent_die_init,
            (tmp, logpath))

        with printlog(logpath), force_kill_on_exit(proc):

            res = get_queue(url).getpid()
            assert res.wait(WAIT), repr(res)

            os.kill(proc.pid, signal.SIGKILL)  # force kill pool master
            eventually(proc.is_alive, False, timeout=WAIT)

        try:
            eventually(pid_running(res.value), False,
                timeout=WAIT, poll_interval=0.1)
        except Exception:
            os.kill(res.value, signal.SIGTERM)  # clean up
            raise


def WorkerPool_worker_shutdown_on_parent_die_init(url, tmp, logpath):
    process_config(logpath, 'Worker-%s' % os.getpid())
    broker = get_broker(url)

    broker.expose(os.getpid)

    import worq.pool.process as proc
    proc.WORKER_POLL_INTERVAL = 0.1

    return broker

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# pool test helpers


def process_config(logpath, procname):
    import worq.pool.process as proc
    proc.WORKER_POLL_INTERVAL = 1
    logging.config.dictConfig({
        'formatters': {
            'brief': {
                'format': ('%(asctime)s ' + procname
                    + ' %(levelname)-7s %(name)s - %(message)s'),
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.FileHandler',
                'filename': logpath,
                'mode': 'a',
                'formatter': 'brief',
            },
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console'],
        },
        'disable_existing_loggers': False,
        'version': 1,
    })
    log.info('WORKER_POLL_INTERVAL = %s', proc.WORKER_POLL_INTERVAL)


def touch(path, data=''):
    with open(path, 'w') as f:
        f.write(data)


def reader(*path):
    path = join(*path)

    def read():
        if exists(path):
            with open(path) as f:
                return f.read()
    return read


def verify_shutdown(proc):
    def verify():
        return not proc.is_alive()
    return verify


def pid_running(pid):
    def is_process_running(pid=pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    return is_process_running


@contextmanager
def printlog(logpath, heading='pool logging output:'):
    try:
        yield
    finally:
        if exists(logpath):
            print(heading)
            with open(logpath) as f:
                sys.stdout.write(f.read())


def _logging_init(url, _logpath, _init, *args, **kw):
    if exists(dirname(_logpath)):
        process_config(_logpath, 'Worker-%s' % os.getpid())
    else:
        # worker was probably orphaned
        sys.exit()
    broker = _init(url, *args, **kw)
    broker.expose(noop)
    return broker


def noop():
    pass


def worker_pool(url, init_func, init_args, workers=1):
    process_config(init_args[-1], 'Pool-%s' % os.getpid())
    broker = get_broker(url)
    with discard_tasks(broker):
        pool = WorkerPool(broker, init_func, init_args, workers=workers)
        pool.start(timeout=1)


@contextmanager
def start_pool(pool, timeout=1):
    with tempdir() as tmp:
        assert exists(tmp), tmp
        logpath = join(tmp, 'log')
        pool.init_kwargs.setdefault('_logpath', logpath)
        pool.init_kwargs.setdefault('_init', pool.init_func)
        pool.init_func = _logging_init
        with discard_tasks(pool.broker), printlog(logpath):
            pool.start(timeout=timeout, handle_sigterm=False)
            try:
                yield
            finally:
                if not pool.broker.url.startswith('memory:'):
                    pool.broker.queue().noop()
                pool.stop()


@contextmanager
def discard_tasks(broker):
    try:
        yield
    finally:
        broker.discard_pending_tasks()


@contextmanager
def force_kill_on_exit(proc):
    try:
        yield
    except:
        log.error('original error', exc_info=True)
        raise
    finally:
        if proc.is_alive():
            proc.join(WAIT)
            if proc.is_alive():
                # force kill
                os.kill(proc.pid, signal.SIGKILL)
                raise RuntimeError('Had to force kill broker process. '
                    'Worker subprocesses may be orphaned.')
