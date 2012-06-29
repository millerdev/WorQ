# PyMQ examples
from pymq import get_broker, Queue, Task, TaskSet, TaskFailure, TaskSpace
from pymq.tests.test_examples import example
from pymq.tests.util import assert_raises, eq_, eventually, thread_worker


@example
def simple(url):
    state = []

    def func(arg):
        state.append(arg)

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        q.func('arg')

        eventually((lambda:state), ['arg'])


@example
def expose_method(url):

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
    broker = get_broker(url)
    broker.expose(obj.update_value)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)
        q.update_value(2)

        eventually((lambda:db.value), 2)


@example
def busy_wait(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        func_task = Task(q.func, result_timeout=3)
        res = func_task('arg')

        completed = res.wait(timeout=1, poll_interval=0)

        assert completed, repr(res)
        eq_(res.value, 'arg')
        eq_(repr(res), "<DeferredResult %s success>" % res.id)


@example
def no_such_task(url):

    broker = get_broker(url)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')

        completed = res.wait(timeout=1, poll_interval=0)

        assert completed, repr(res)
        eq_(repr(res), '<DeferredResult %s failed>' % res.id)
        with assert_raises(TaskFailure,
                'no such task: func [default:%s]' % res.id):
            res.value


@example
def worker_interrupted(url):

    def func(arg):
        raise KeyboardInterrupt()

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        completed = res.wait(timeout=1, poll_interval=0)

        assert completed, repr(res)
        eq_(repr(res), '<DeferredResult %s failed>' % res.id)
        with assert_raises(TaskFailure, 'KeyboardInterrupt: '):
            res.value


@example
def task_error(url):

    def func(arg):
        raise Exception('fail!')

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        res = Task(q.func, result_timeout=3)('arg')
        completed = res.wait(timeout=1, poll_interval=0)

        assert completed, repr(res)
        eq_(repr(res), "<DeferredResult %s failed>" % res.id)
        with assert_raises(TaskFailure, 'Exception: fail!'):
            res.value


@example
def taskset(url):

    def func(arg):
        return arg

    broker = get_broker(url, 'not-the-default-queue')
    broker.expose(func)
    broker.expose(sum)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, name='not-the-default-queue')

        tasks = TaskSet(result_timeout=5)
        tasks.add(q.func, 1)
        tasks.add(q.func, 2)
        tasks.add(q.func, 3)
        res = tasks(q.sum)

        eventually((lambda: res.value if res else None), 6)


@example
def taskset_composition(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    broker.expose(sum)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        set_0 = TaskSet()
        set_0.add(q.func, 1)
        set_0.add(q.func, 2)
        set_0.add(q.func, 3)

        set_1 = TaskSet(result_timeout=5)
        set_1.add(q.func, 4)
        set_1.add(set_0, q.sum)
        set_1.add(q.func, 5)

        res = set_1(q.sum)

        eventually((lambda: res.value if res else None), 15)


@example
def taskset_with_errors(url):

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        tasks = TaskSet(result_timeout=5) #, on_error=TaskSet.RAISE)
        tasks.add(q.func, 1)
        tasks.add(q.func, 0)
        tasks.add(q.func, 2)
        res = tasks(q.func)
        res.wait(timeout=1, poll_interval=0)

        eq_(res.value, [1, TaskFailure('Exception: zero fail!'), 2])


@example
def task_namespaces(url):
    state = []
    __name__ = 'module.path'

    ts = TaskSpace(__name__)

    @ts.task
    def foo():
        state.append('foo')

    @ts.task
    def bar(arg):
        state.append(arg)

    broker = get_broker(url)
    broker.expose(ts)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url, 'module.path')

        q.foo()
        q.bar(1)

        eventually((lambda:len(state) == 2 and state), ['foo', 1])


@example
def more_namespaces(url):
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

    broker = get_broker(url)
    broker.expose(foo)
    broker.expose(bar)
    broker.expose(baz)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = Queue(url)

        q.foo.join(1)
        q.foo.bar.kick(2)
        q.foo.bar.baz.join(3)
        q.foo.bar.baz.kick(4)

        eventually((lambda:len(state) == 4 and state), [
            'foo-join 1',
            'bar-kick 2',
            'baz-join 3',
            'baz-kick 4',
        ])
