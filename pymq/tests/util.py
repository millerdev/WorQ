import functools
import time
import logging
from contextlib import contextmanager
from threading import Thread, Lock

from pymq.core import _StopWorker

log = logging.getLogger(__name__)

TEST_URLS = [
    'memory://',
    'redis://localhost:16379/0', # NOTE non-standard port
]

def with_urls(test):
    @functools.wraps(test)
    def wrapper():
        for url in TEST_URLS:
            yield test, url
    return wrapper

@contextmanager
def thread_worker(broker, lock=None):
    if lock is None:
        def run():
            try:
                broker.start_worker()
            except:
                log.error('worker crashed', exc_info=True)
    else:
        # pause worker prior to invoking each task
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
        if lock is not None:
            lock.release()
        t.join()
        broker.discard_pending_tasks()

class TimeoutLock(object):
    """A lock with acquisition timeout"""
    def __init__(self, locked=True):
        self.lock = Lock()
        if locked:
            self.lock.acquire()
    def acquire(self, timeout=1):
        end = time.time() + timeout
        while time.time() < end:
            if self.lock.acquire(False):
                return
        raise Exception('lock timeout')
    def release(self):
        self.lock.release()

class StepLock(TimeoutLock):
    """A lock for temporarily blocking queue processing"""
    def __init__(self, locked=True):
        super(StepLock, self).__init__(locked)
        self.step_lock = TimeoutLock()
    def acquire(self, timeout=1):
        super(StepLock, self).acquire(timeout)
        self.step_lock.release()
    def step(self):
        """Release lock and wait until it is next acquired."""
        self.release()
        self.step_lock.acquire()

def eventually(get_value, value, timeout=1):
    end = time.time() + timeout
    while time.time() < end:
        result = get_value()
        if result == value:
            return
    raise AssertionError('eventually timeout: %r != %r' % (result, value))

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
