import logging
import logging.config
import os
import signal
import sys
from contextlib import contextmanager
from os.path import exists, join
from pymq import queue
from pymq.procpool import WorkerPool, run_in_subprocess
from pymq.tests.util import assert_raises, eq_, eventually, tempdir, with_urls


def worker_pool(url, init_func, init_args):
    logging_config(init_args[-1], 'Broker-%s' % os.getpid())

    pool = WorkerPool(url, get_task_timeout=1)
    pool.start(init_func, *init_args)


def init_worker(broker, tmp, logpath):
    logging_config(logpath, 'Worker-%s' % os.getpid())

    @broker.expose
    def func(arg, lock=None):
        # signal func started
        touch(join(tmp, 'func.started'))

        # wait for unlock
        eventually(reader(tmp, 'func.unlock'), '')

        # write data to file
        touch(join(tmp, 'func.out'), arg)


@with_urls(exclude='memory')
def test_WorkerPool_terminate(url):
    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url, init_worker, (tmp, logpath))

        with printlog(logpath), force_kill_on_exit(proc):

            # -- task-invoking code, usually another process --
            q = queue(url)
            q.func('text')

            eventually(reader(tmp, 'func.started'), '')

            proc.terminate() # signal pool shutdown
            touch(join(tmp, 'func.unlock')) # allow func to proceed

            eventually(reader(tmp, 'func.out'), 'text')
            eventually(verify_shutdown(proc), True, timeout=10)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# pool test helpers

def logging_config(logpath, procname):
    logging.config.dictConfig({
        'formatters': {
            'brief': {
                'format': procname + ' %(levelname)-7s %(name)s - %(message)s',
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

@contextmanager
def printlog(logpath, heading='pool logging output:'):
    try:
        yield
    finally:
        if exists(logpath):
            print heading
            with open(logpath) as f:
                print f.read(),

@contextmanager
def force_kill_on_exit(proc):
    try:
        yield
    finally:
        if proc.is_alive():
            proc.join(10)
            if proc.is_alive():
                # force kill
                os.kill(proc.pid, signal.SIGKILL)
                raise RuntimeError('Had to force kill broker process. '
                    'Some worker processes may be orphaned.')
