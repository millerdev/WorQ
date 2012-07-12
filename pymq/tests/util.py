import functools
import time
import logging
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp
from threading import Thread, Event

from pymq.core import _StopWorker

log = logging.getLogger(__name__)

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
def thread_worker(broker, event=None):
    if event is None:
        def run():
            try:
                broker.start_worker()
            except:
                log.error('worker crashed', exc_info=True)
    else:
        # wait for event to be set prior to invoking each task
        def run():
            try:
                for queue, message in broker.messages:
                    event.wait()
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
        if event is not None:
            event.set()
        t.join()
        broker.discard_pending_tasks()

class TempEvent(object):
    """An event with a default wait timeout of one second."""
    def __init__(self):
        self.event = Event()
    def set(self):
        self.event.set()
    def wait(self, timeout=1):
        """Wait for the event to be set, then clear the event.

        :param timeout: Seconds to wait prior to raising an exception.
        :raises: Exception on timeout.
        """
        if self.event.wait(timeout):
            self.event.clear()
            return
        raise Exception('event timeout')

def eventually(get_value, value, timeout=1):
    end = time.time() + timeout
    while time.time() < end:
        result = get_value()
        if result == value:
            return
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
