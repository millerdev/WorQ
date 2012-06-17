# PyMQ examples
import logging
import time
from contextlib import contextmanager
from threading import Thread
from pymq import Broker, Queue, TaskSet, TaskSpace

log = logging.getLogger(__name__)
test_urls = {
    'memory': 'memory://',
    'redis': 'redis://localhost:6379/0',
}


def simple(url, run_worker):
    state = []

    def func(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(func)
    with run_worker(broker):

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
        q.update_value(2)

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


def namespaces(url, run_worker):
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
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        foo = Queue(url, 'foo')

        foo.join()
        foo.bar.kick(1)
        foo.bar.baz.join()
        foo.bar.baz.kick(2)

        eventually((lambda:len(state) == 4 and state), ['join', 1, 'join', 2])


def decorator(url, run_worker):
    state = []
    __name__ = 'module.path'

    ts = TaskSpace(__name__)

    @ts.task
    def join():
        state.append('join')

    @ts.task
    def kick(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(ts)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, 'module.path')

        q.join()
        q.kick(1)

        eventually((lambda:len(state) == 2 and state), ['join', 1])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# testing helpers

examples = [
    simple,
    method_publishing,
    taskset,
    exception_in_task,
    namespaces,
    decorator,
]

def test_memory():
    url = test_urls['memory']
    broker = Broker(url) # keep a reference so we always get the same one
    @contextmanager
    def run_worker(broker):
        broker.start_worker() # NOOP
        yield

    for example in examples:
        yield example, url, run_worker

def test_redis():
    url = test_urls['redis']
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
