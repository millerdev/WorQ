import logging
import logging.config
import os
import signal
import sys
from contextlib import contextmanager
from nose.tools import nottest
from os.path import exists, join
from pymq import get_broker, queue
from pymq.procpool import WorkerPool, Error, run_in_subprocess
from pymq.task import Task, TaskSet
from pymq.tests.util import assert_raises, eq_, eventually, tempdir, with_urls

log = logging.getLogger(__name__)


def worker_pool(url, init_func, init_args, workers=1):
    logging_config(init_args[-1], 'Broker-%s' % os.getpid())

    with discard_tasks(url):
        pool = WorkerPool(url, get_task_timeout=1, workers=workers)
        pool.start(init_func, init_args)


@with_urls(exclude='memory')
def test_WorkerPool_sigterm(url):

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

            log.debug('func complete')

        @broker.expose
        def noop():
            pass

    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(
            worker_pool, url, init_worker, (tmp, logpath), workers=3)

        with printlog(logpath), force_kill_on_exit(proc):

            q = queue(url)

            # send enough tasks to start all workers
            for x in range(10):
                q.noop()

            q.func('text')

            eventually(reader(tmp, 'func.started'), '')

            proc.terminate() # signal pool shutdown
            touch(join(tmp, 'func.unlock')) # allow func to proceed

            eventually(reader(tmp, 'func.out'), 'text')
            eventually(verify_shutdown(proc), True, timeout=10)


@with_urls(exclude='memory')
def test_WorkerPool_start_twice(url):
    init_worker = lambda url:None

    pool = WorkerPool(url, get_task_timeout=1, workers=1)
    with start_pool(pool, init_worker):
        with assert_raises(Error):
            pool.start(init_worker, handle_sigterm=False)


@with_urls(exclude='memory')
def test_WorkerPool_max_worker_tasks(url):
    def init_worker(broker):
        calls = [0]

        @broker.expose
        def func():
            calls[0] += 1
            return (os.getpid(), calls[0])

        @broker.expose
        def results(res):
            return res

    pool = WorkerPool(url, get_task_timeout=1, workers=1, max_worker_tasks=3)
    with start_pool(pool, init_worker):

        q = queue(url)

        t = TaskSet(result_timeout=3)
        for n in range(5):
            t.add(q.func)

        res = t(q.results)
        assert res.wait(timeout=10, poll_interval=0.01), 'task not completed'

        results = res.value
        assert isinstance(results, list), results
        eq_([r[1] for r in results], [1, 2, 3, 1, 2])
        eq_(len(set(r[0] for r in results)), 2)


@nottest # this is a very slow test, and doesn't test that much
@with_urls(exclude='memory')
def test_WorkerPool_crashed_worker(url):
    def init_worker(broker):

        @broker.expose
        def kill_worker():
            log.warn('alone we crash')
            sys.exit()

        @broker.expose
        def pid():
            return os.getpid()

    pool = WorkerPool(url, get_task_timeout=1, workers=1)
    with start_pool(pool, init_worker):

        q = queue(url)

        res = Task(q.pid, result_timeout=3)()
        assert res.wait(timeout=10, poll_interval=0.01), 'first not completed'
        pid = res.value

        q.kill_worker()

        res = Task(q.pid, result_timeout=3)()
        assert res.wait(timeout=20, poll_interval=0.1), 'second not completed'
        assert res.value != pid, pid


@with_urls(exclude='memory')
def test_WorkerPool_worker_shutdown_on_parent_die(url):
    def pid_running(pid):
        def is_process_running(pid=pid):
            try:
                os.kill(pid, 0)
            except OSError:
                return False
            return True
        return is_process_running

    def init_worker(broker, tmp, logpath):
        logging_config(logpath, 'Worker-%s' % os.getpid())

        @broker.expose
        def pid():
            return os.getpid()

        import pymq.procpool
        pymq.procpool.WORKER_POLL_INTERVAL = 0.1

    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url, init_worker, (tmp, logpath))

        with printlog(logpath), force_kill_on_exit(proc):

            q = queue(url)

            res = Task(q.pid, result_timeout=10)()
            assert res.wait(timeout=30, poll_interval=0.01), 'not completed'

            os.kill(proc.pid, signal.SIGKILL) # force kill pool broker
            eventually(proc.is_alive, False, timeout=10)

        try:
            eventually(pid_running(res.value), False,
                timeout=15, poll_interval=0.1)
        except Exception:
            os.kill(res.value, signal.SIGTERM) # clean up
            raise

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# pool test helpers

def logging_config(logpath, procname):
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
def start_pool(pool, init_worker, init_args=(), init_kw=None, **kw):
    def logging_init(broker, _logpath, _init, *args, **kw):
        logging_config(_logpath, 'Worker-%s' % os.getpid())
        _init(broker, *args, **kw)
    if init_kw is None:
        init_kw = {}
    with tempdir() as tmp:
        logpath = join(tmp, 'log')
        init_kw.update(_logpath=logpath, _init=init_worker)
        with discard_tasks(pool.broker_url), printlog(logpath):
            pool.start(logging_init, init_args, init_kw,
                    handle_sigterm=False, **kw)
            try:
                yield
            finally:
                pool.stop()

@contextmanager
def discard_tasks(url):
    #broker = get_broker(url)
    #broker.discard_pending_tasks()
    try:
        yield
    finally:
        get_broker(url).discard_pending_tasks()

@contextmanager
def force_kill_on_exit(proc):
    try:
        yield
    except:
        log.error('original error', exc_info=True)
        raise
    finally:
        if proc.is_alive():
            proc.join(10)
            if proc.is_alive():
                # force kill
                os.kill(proc.pid, signal.SIGKILL)
                raise RuntimeError('Had to force kill broker process. '
                    'Some worker processes may be orphaned.')
