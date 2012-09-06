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

import functools
import time
import logging
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp
from threading import Lock

from worq.pool.thread import WorkerPool
from worq.tests import TEST_URLS

log = logging.getLogger(__name__)

WAIT = 30 # default test timeout (seconds)
DEFAULT_TIMEOUT = 10 # default utility timeout

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
def thread_worker(broker, lock=None, timeout=1):
    if lock is not None:
        real_next_task = broker.next_task
        def next_task(*args, **kw):
            log.debug('acquiring lock before getting next task')
            lock.acquire()
            return real_next_task(*args, **kw)
        broker.next_task = next_task
    pool = WorkerPool(broker)
    pool.start(timeout=timeout)
    try:
        yield
    finally:
        pool.stop(use_sentinel=True, join=False)
        try:
            if lock is not None:
                lock.release()
        except Exception:
            log.error('lock release failed', exc_info=True)
        finally:
            pool.join()
            broker.discard_pending_tasks()

class TimeoutLock(object):
    """A lock with acquisition timeout"""
    def __init__(self, locked=False):
        self.lock = Lock()
        self.mutex = Lock()
        with self.mutex:
            if locked:
                self.lock.acquire()
            self.locked = locked
    def acquire(self, timeout=DEFAULT_TIMEOUT):
        end = time.time() + timeout
        while time.time() < end:
            with self.mutex:
                if self.lock.acquire(False):
                    self.locked = True
                    return
        raise Exception('lock timeout')
    def release(self):
        with self.mutex:
            self.lock.release()
            self.locked = False

def eventually(get_value, expect, timeout=DEFAULT_TIMEOUT, poll_interval=0):
    end = time.time() + timeout
    while time.time() < end:
        actual = get_value()
        if actual == expect:
            return
        time.sleep(poll_interval)
    raise AssertionError('eventually timeout: %r != %r' % (actual, expect))

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
        if isinstance(msg, basestring):
            eq_(str(exc), msg)
        elif msg is not None:
            msg.search(str(exc))
    else:
        raise AssertionError('%s not raised' % exc_class.__name__)

def eq_(value, other):
    assert value == other, '%r != %r' % (value, other)
