import functools
import time
import logging
from threading import Thread
from contextlib import contextmanager

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
def thread_worker(broker):
    def run():
        try:
            broker.start_worker()
        except:
            log.error('worker crashed', exc_info=True)
    t = Thread(target=run)
    t.start()
    try:
        yield
    finally:
        broker.stop()
        t.join()
        broker.discard_pending_tasks()

def eventually(condition, value, timeout=1):
    end = time.time() + timeout
    while time.time() < end:
        result = condition()
        if result:
            eq_(result, value)
            return
    raise AssertionError('eventuality failed to occur: %r' % (value,))

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
