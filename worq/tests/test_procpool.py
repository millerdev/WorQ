import logging
import logging.config
import os
import signal
import sys
from contextlib import contextmanager
from nose.tools import nottest
from os.path import dirname, exists, join
from worq import get_broker, queue
from worq.procpool import WorkerPool, Error, run_in_subprocess
from worq.task import Task, TaskSet
from worq.tests.util import assert_raises, eq_, eventually, tempdir, with_urls

log = logging.getLogger(__name__)

WAIT = 60 # default wait timeout (1 minute)

def worker_pool(url, init_func, init_args, workers=1):
    process_config(init_args[-1], 'Broker-%s' % os.getpid())

    with discard_tasks(url):
        pool = WorkerPool(url, workers=workers, get_task_timeout=1)
        pool.start(init_func, init_args)


@with_urls(exclude='memory')
def test_WorkerPool_sigterm(url):
    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url,
            WorkerPool_sigterm_init_worker, (tmp, logpath), workers=3)

        with printlog(logpath), force_kill_on_exit(proc):

            q = queue(url)

            q.func('text')

            eventually(reader(tmp, 'func.started'), '')

            proc.terminate() # signal pool shutdown
            touch(join(tmp, 'func.unlock')) # allow func to proceed

            eventually(reader(tmp, 'func.out'), 'text')
            eventually(verify_shutdown(proc), True, timeout=WAIT)

def WorkerPool_sigterm_init_worker(url, tmp, logpath):
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
def test_WorkerPool_start_twice(url):
    pool = WorkerPool(url, workers=1, get_task_timeout=1)
    with start_pool(pool, WorkerPool_start_twice_init_worker):
        with assert_raises(Error):
            pool.start(WorkerPool_start_twice_init_worker, handle_sigterm=False)

def WorkerPool_start_twice_init_worker(url):
    return get_broker(url)


@with_urls(exclude='memory')
def test_WorkerPool_max_worker_tasks(url):
    pool = WorkerPool(url, workers=1, get_task_timeout=1, max_worker_tasks=3)
    with start_pool(pool, WorkerPool_max_worker_tasks_init_worker):

        q = queue(url)

        t = TaskSet(result_timeout=WAIT)
        for n in range(4):
            t.add(q.func)

        res = t(q.results)
        assert res.wait(WAIT), repr(res)

        results = res.value
        assert isinstance(results, list), results
        eq_([r[1] for r in results], [1, 2, 3, 1])
        eq_(len(set(r[0] for r in results)), 2)

def WorkerPool_max_worker_tasks_init_worker(url):
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


@nottest # this is a very slow test, and doesn't seem that important
@with_urls(exclude='memory')
def test_WorkerPool_crashed_worker(url):
    pool = WorkerPool(url, workers=1, get_task_timeout=1)
    with start_pool(pool, WorkerPool_crashed_worker_init_worker):

        q = queue(url)

        res = Task(q.getpid, result_timeout=WAIT)()
        assert res.wait(WAIT), repr(res)
        pid = res.value

        q.kill_worker()

        res = Task(q.getpid, result_timeout=WAIT)()
        assert res.wait(WAIT), repr(res)
        assert res.value != pid, pid

def WorkerPool_crashed_worker_init_worker(url):
    broker = get_broker(url)

    @broker.expose
    def kill_worker():
        log.warn('alone we crash')
        sys.exit()

    broker.expose(os.getpid)

    return broker


@with_urls(exclude='memory')
def test_WorkerPool_worker_shutdown_on_parent_die(url):
    with tempdir() as tmp:

        logpath = join(tmp, 'output.log')
        proc = run_in_subprocess(worker_pool, url,
            WorkerPool_worker_shutdown_on_parent_die_init_worker,
            (tmp, logpath))

        with printlog(logpath), force_kill_on_exit(proc):

            res = Task(queue(url).getpid, result_timeout=WAIT)()
            assert res.wait(WAIT), repr(res)

            os.kill(proc.pid, signal.SIGKILL) # force kill pool master
            eventually(proc.is_alive, False, timeout=WAIT)

        try:
            eventually(pid_running(res.value), False,
                timeout=WAIT, poll_interval=0.1)
        except Exception:
            os.kill(res.value, signal.SIGTERM) # clean up
            raise

def WorkerPool_worker_shutdown_on_parent_die_init_worker(url, tmp, logpath):
    process_config(logpath, 'Worker-%s' % os.getpid())
    broker = get_broker(url)

    broker.expose(os.getpid)

    import worq.procpool
    worq.procpool.WORKER_POLL_INTERVAL = 0.1

    return broker

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# pool test helpers

def process_config(logpath, procname):
    import worq.procpool
    worq.procpool.WORKER_POLL_INTERVAL = 1
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
    log.info('WORKER_POLL_INTERVAL = %s', worq.procpool.WORKER_POLL_INTERVAL)

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
            print heading
            with open(logpath) as f:
                print f.read(),

def _logging_init(url, _logpath, _init, *args, **kw):
    if exists(dirname(_logpath)):
        process_config(_logpath, 'Worker-%s' % os.getpid())
    else:
        # worker was probably orphaned
        sys.exit()
    return _init(url, *args, **kw)

@contextmanager
def start_pool(pool, init_worker, init_args=(), init_kw=None, **kw):
    if init_kw is None:
        init_kw = {}
    with tempdir() as tmp:

        assert exists(tmp), tmp

        logpath = join(tmp, 'log')
        init_kw.update(_logpath=logpath, _init=init_worker)
        with discard_tasks(pool.broker_url), printlog(logpath):
            pool.start(_logging_init, init_args, init_kw,
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
            proc.join(WAIT)
            if proc.is_alive():
                # force kill
                os.kill(proc.pid, signal.SIGKILL)
                raise RuntimeError('Had to force kill broker process. '
                    'Worker subprocesses may be orphaned.')
