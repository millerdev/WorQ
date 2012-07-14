import functools
import time
import logging
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp
from threading import Thread, Lock

from pymq.core import _StopWorker

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10

TEST_URLS = [
    'memory://',
    'redis://localhost:16379/0', # NOTE non-standard port
]

def with_urls(test=None, exclude=None):
    if test is not None:
        @functools.wraps(test)
        def wrapper():
            for url in TEST_URLS:
                if exclude:
                    if not url.startswith(exclude):
                        yield test, url
                else:
                    yield test, url
        return wrapper
    return functools.partial(with_urls, exclude=exclude)

@contextmanager
def thread_worker(broker, lock=None):
    if lock is None:
        def run():
            try:
                broker.start_worker()
            except:
                log.error('worker crashed', exc_info=True)
    else:
        # acquire lock prior to invoking each task
        def run():
            try:
                for queue, message in broker.messages:
                    lock.acquire()
                    broker.invoke(queue, message)
            except _StopWorker:
                log.info('worker stopped')
            except:
                log.error('worker crashed', exc_info=True)
    t = Thread(target=run)
    t.start()
    try:
        yield
    finally:
        broker.stop()
        try:
            if lock is not None:
                lock.release()
        except Exception:
            log.error('lock release failed', exc_info=True)
        finally:
            t.join()
            broker.discard_pending_tasks()

class TimeoutLock(object):
    """A lock with acquisition timeout; initially locked by default."""
    def __init__(self, locked=True):
        self.lock = Lock()
        if locked:
            self.lock.acquire()
    def acquire(self, timeout=DEFAULT_TIMEOUT):
        end = time.time() + timeout
        while time.time() < end:
            if self.lock.acquire(False):
                return
        raise Exception('lock timeout')
    def release(self):
        self.lock.release()

def eventually(get_value, value, timeout=DEFAULT_TIMEOUT, poll_interval=0):
    end = time.time() + timeout
    while time.time() < end:
        result = get_value()
        if result == value:
            return
        time.sleep(poll_interval)
    raise AssertionError('eventually timeout: %r != %r' % (result, value))

@contextmanager
def tempdir(*args, **kw):
    """Create a temporary directory to be used in a test

    If (optional keyword argument) 'delete' evaluates to True (the default
    value), the temporary directory and all files in it will be removed on
    context manager exit.
    """
    delete = kw.pop("delete", True)
    path = mkdtemp(*args, **kw)
    try:
        yield path
    finally:
        if delete:
            shutil.rmtree(path)

@contextmanager
def assert_raises(exc_class, msg=None):
    try:
        yield
    except exc_class as exc:
        if msg is not None:
            eq_(str(exc), msg)
    else:
        raise AssertionError('%s not raised' % exc_class.__name__)

def eq_(value, other):
    assert value == other, '%r != %r' % (value, other)
