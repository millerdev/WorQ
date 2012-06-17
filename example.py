# PyMQ examples
import logging
import time
from contextlib import contextmanager
from threading import Thread
from pymq import Broker, Queue, TaskSet

log = logging.getLogger(__name__)


def simple(url, thread_worker):
    state = []

    def func(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)
        res = q.func('arg')
        eq_(res, None)

        eventually((lambda:state), ['arg'])


def method_publishing(url, run_worker):

    class Database(object):
        """stateful storage"""
        value = None
        def update(self, value):
            self.value = value
    class TaskObj(object):
        """object with task definitions"""
        def __init__(self, db):
            self.db = db
        def update_value(self, value):
            self.db.update(value)

    db = Database()
    obj = TaskObj(db)
    broker = Broker(url)
    broker.publish(obj.update_value)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)
        res = q.update_value(2)
        eq_(res, None)

        eventually((lambda:db.value), 2)


def taskset(url, run_worker):

    def func(arg):
        return arg
    def get_number(num):
        return num

    broker = Broker(url, 'not-the-default-queue')
    broker.publish(get_number)
    broker.publish(sum)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, name='not-the-default-queue')
        tasks = TaskSet(result_timeout=5)
        for n in [1, 2, 3]:
            tasks.add(q.get_number, n)

        res = tasks(q.sum)

        assert res is not None
        eventually((lambda: res.value if res else None), 6)


def exception_in_task(url, run_worker):
    state = []

    def bad():
        raise SystemExit('something bad happened')
    def good(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(bad)
    broker.publish(good)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        eq_(q.bad(), None)
        eq_(q.good(1), None)

        eventually((lambda: state), [1])


def namespaces(url, thread_worker):
    state = []

    def join():
        state.append('join')
    def kick(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(join, 'foo')
    broker.publish(kick, 'foo.bar')
    broker.publish(join, 'foo.bar.baz')
    broker.publish(kick, 'foo.bar.baz')
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        broker = Broker(url)
        foo = Queue(url, 'foo')

        foo.join()
        foo.bar.kick(1)
        foo.bar.baz.join()
        foo.bar.baz.kick(2)

        eventually((lambda:len(state) == 4 and state), ['join', 1, 'join', 2])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# testing helpers (probably not very meaningful example code)

examples = [
    simple,
    method_publishing,
    taskset,
    exception_in_task,
    namespaces,
]

def test_memory():
    url = 'memory://'
    broker = Broker(url) # keep a reference so we always get the same one
    @contextmanager
    def run_worker(broker):
        broker.start_worker() # NOOP
        yield

    for example in examples:
        yield example, url, run_worker

def test_redis():
    url = 'redis://localhost:6379/0'
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
            if t.is_alive():
                broker.stop()
                t.join()

    for example in examples:
        yield example, url, thread_worker

def eventually(condition, value, timeout=1):
    end = time.time() + timeout
    while time.time() < end:
        result = condition()
        if result:
            eq_(result, value)
            return
    raise AssertionError('eventuality failed to occur: %r' % (value,))

def eq_(value, other):
    assert value == other, '%r != %r' % (value, other)
