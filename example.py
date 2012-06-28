# PyMQ examples
import logging
import time
from contextlib import contextmanager
from threading import Thread
from pymq import Broker, Queue, Task, TaskSet, TaskSpace

log = logging.getLogger(__name__)
test_urls = [
    'memory://',
    'redis://localhost:16379/0', # NOTE non-standard port
]

def example(func):
    example.s.append(func)
    return func
example.s = []


@example
def simple(url, run_worker):
    state = []

    def func(arg):
        state.append(arg)

    broker = Broker(url)
    broker.expose(func)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = q.func('arg')

        eq_(res, None)
        eventually((lambda:state), ['arg'])


@example
def expose_method(url, run_worker):

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
    broker.expose(obj.update_value)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)
        q.update_value(2)

        eventually((lambda:db.value), 2)


@example
def busy_wait(url, run_worker):

    def func(arg):
        return arg

    broker = Broker(url)
    broker.expose(func)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        wait_result = res.wait(timeout=1, poll_interval=0)

        assert wait_result, repr(res)
        eq_(res.value, 'arg')


@example
def no_such_task(url, run_worker):

    broker = Broker(url)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        res.wait(timeout=1, poll_interval=0)

        tid = res.task_id
        eq_(repr(res), '<DeferredResult no such task: func [default:%s]>' % tid)
        eq_(res.error, 'no such task: func [default:%s]' % tid)
        assert not hasattr(res, 'value'), repr(res)


@example
def worker_interrupted(url, run_worker):

    def func(arg):
        raise KeyboardInterrupt()

    broker = Broker(url)
    broker.expose(func)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        res.wait(timeout=1, poll_interval=0)

        eq_(repr(res), "<DeferredResult interrupted>")
        eq_(res.error, 'interrupted')
        assert not hasattr(res, 'value'), repr(res)


@example
def task_error(url, run_worker):

    def func(arg):
        raise Exception('fail!')

    broker = Broker(url)
    broker.expose(func)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        res.wait(timeout=1, poll_interval=0)

        eq_(repr(res), "<DeferredResult Exception: fail!>")
        eq_(res.error, 'Exception: fail!')
        assert not hasattr(res, 'value'), repr(res)


@example
def taskset(url, run_worker):

    def func(arg):
        return arg

    broker = Broker(url, 'not-the-default-queue')
    broker.expose(func)
    broker.expose(sum)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, name='not-the-default-queue')

        tasks = TaskSet(result_timeout=5)
        for n in [1, 2, 3]:
            tasks.add(q.func, n)
        res = tasks(q.sum)

        assert res is not None
        eventually((lambda: res.value if res else None), 6)


@example
def taskset_composition(url, run_worker):

    def func(arg):
        return arg

    broker = Broker(url)
    broker.expose(func)
    broker.expose(sum)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        set_0 = TaskSet()
        for n in [1, 2, 3]:
            set_0.add(q.func, n)

        set_1 = TaskSet(result_timeout=5)
        set_1.add(set_0, q.sum)
        for n in [4, 5]:
            set_1.add(q.func, n)

        res = set_1(q.sum)

        eventually((lambda: res.value if res else None), 15)


@example
def exception_in_task(url, run_worker):
    state = []

    def bad():
        raise SystemExit('something bad happened')
    def good(arg):
        state.append(arg)

    broker = Broker(url)
    broker.expose(bad)
    broker.expose(good)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        eq_(q.bad(), None)
        eq_(q.good(1), None)

        eventually((lambda: state), [1])


@example
def task_namespaces(url, run_worker):
    state = []
    __name__ = 'module.path'

    ts = TaskSpace(__name__)

    @ts.task
    def foo():
        state.append('foo')

    @ts.task
    def bar(arg):
        state.append(arg)

    broker = Broker(url)
    broker.expose(ts)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, 'module.path')

        q.foo()
        q.bar(1)

        eventually((lambda:len(state) == 2 and state), ['foo', 1])


@example
def more_namespaces(url, run_worker):
    state = []

    foo = TaskSpace('foo')
    bar = TaskSpace('foo.bar')
    baz = TaskSpace('foo.bar.baz')

    @foo.task
    def join(arg):
        state.append('foo-join %s' % arg)

    @bar.task
    def kick(arg):
        state.append('bar-kick %s' % arg)

    @baz.task
    def join(arg):
        state.append('baz-join %s' % arg)

    @baz.task
    def kick(arg):
        state.append('baz-kick %s' % arg)

    broker = Broker(url)
    broker.expose(foo)
    broker.expose(bar)
    broker.expose(baz)
    with run_worker(broker):

        # -- task-invoking code, usually another process --
        foo = Queue(url, 'foo')

        foo.join(1)
        foo.bar.kick(2)
        foo.bar.baz.join(3)
        foo.bar.baz.kick(4)

        eventually((lambda:len(state) == 4 and state), [
            'foo-join 1',
            'bar-kick 2',
            'baz-join 3',
            'baz-kick 4',
        ])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# testing helpers

def test_examples():
    for url in test_urls:
        for test in example.s:
            yield test, url, thread_worker

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

def eq_(value, other):
    assert value == other, '%r != %r' % (value, other)
