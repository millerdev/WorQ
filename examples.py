# WorQ - asynchronous Python task queue.
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

import worq.const as const
from worq import get_broker, queue, Task, TaskSet, TaskFailure, TaskSpace
from worq.tests.test_examples import example
from worq.tests.util import (assert_raises, eq_, eventually,
    thread_worker, TimeoutLock)

WAIT = 60 # default wait time (1 minute)

@example
def simple(url):
    state = []

    def func(arg):
        state.append(arg)

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

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
        q = queue(url)
        q.update_value(2)

        eventually((lambda:db.value), 2)


@example
def named_queue(url):
    # named queues facilitate multiple queues on a single backend
    state = []

    def func(arg):
        state.append(arg)

    broker = get_broker(url, 'name')
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url, 'name')
        q.func(1)

        eventually((lambda:state), [1])


@example
def wait_for_result(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        func_task = Task(q.func, result_timeout=WAIT)
        res = func_task('arg')

        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(res.value, 'arg')
        eq_(repr(res), "<Deferred func [default:%s] success>" % res.id)


@example
def result_status(url):
    # NOTE a lock is used to control state interactions between the producer
    # and the worker for illustration purposes only. This type of lock-step
    # interaction is not normally needed or even desired.
    lock = TimeoutLock(locked=True)

    def func(arg, update_status):
        lock.acquire()
        update_status([10])
        lock.acquire()
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker, lock):

        # -- task-invoking code, usually another process --
        q = queue(url)

        func_task = Task(q.func, result_status=True)
        res = func_task('arg')

        eventually((lambda:res.status), const.ENQUEUED)
        eq_(repr(res), "<Deferred func [default:%s] enqueued>" % res.id)

        lock.release()
        eventually((lambda:res.status), const.PROCESSING)
        eq_(repr(res), "<Deferred func [default:%s] processing>" % res.id)

        lock.release()
        eventually((lambda:res.status), [10])
        eq_(repr(res), "<Deferred func [default:%s] [10]>" % res.id)

        lock.release()
        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(res.value, 'arg')
        eq_(repr(res), "<Deferred func [default:%s] success>" % res.id)


@example
def no_such_task(url):

    broker = get_broker(url)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        res = Task(q.func, result_timeout=WAIT)('arg')

        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(repr(res), '<Deferred func [default:%s] failed>' % res.id)
        with assert_raises(TaskFailure,
                'func [default:%s] no such task' % res.id):
            res.value


@example
def worker_interrupted(url):

    def func(arg):
        raise KeyboardInterrupt()

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        res = Task(q.func, result_timeout=WAIT)('arg')
        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(repr(res), '<Deferred func [default:%s] failed>' % res.id)
        with assert_raises(TaskFailure,
                'func [default:%s] KeyboardInterrupt: ' % res.id):
            res.value


@example
def task_error(url):

    def func(arg):
        raise Exception('fail!')

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        res = Task(q.func, result_timeout=WAIT)('arg')
        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(repr(res), '<Deferred func [default:%s] failed>' % res.id)
        with assert_raises(TaskFailure,
                'func [default:%s] Exception: fail!' % res.id):
            res.value


@example
def taskset(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    broker.expose(sum)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        tasks = TaskSet(result_timeout=WAIT)
        tasks.add(q.func, 1)
        tasks.add(q.func, 2)
        tasks.add(q.func, 3)
        res = tasks(q.sum)

        assert res.wait(WAIT), repr(res)
        eq_(res.value, 6)


@example
def taskset_composition(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    broker.expose(sum)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        set_0 = TaskSet()
        set_0.add(q.func, 1)
        set_0.add(q.func, 2)
        set_0.add(q.func, 3)

        set_1 = TaskSet(result_timeout=WAIT)
        set_1.add(q.func, 4)
        set_1.add(set_0, q.sum)
        set_1.add(q.func, 5)

        res = set_1(q.sum)

        assert res.wait(WAIT), repr(res)
        eq_(res.value, 15)


@example
def tasksets_ignore_nulls(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        tasks = TaskSet(result_timeout=WAIT)
        tasks.add(q.func, 1)
        tasks.add(q.func, None)
        tasks.add(q.func, 3)
        tasks.add(q.func, None)
        tasks.add(q.func, 5)

        res = tasks() # call taskset without args returns all results

        assert res.wait(WAIT), repr(res)
        eq_(res.value, [1, 3, 5])


@example
def taskset_with_failed_subtasks(url):
    """TaskSet with TaskFailures passed to the final task

    By default, a TaskSet fails if any of its subtasks fail. However, setting
    the `on_error=TaskSet.PASS` option on the TaskSet will cause TaskFailure
    objects to be passed as the result of any task that fails.
    """

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        tasks = TaskSet(result_timeout=WAIT, on_error=TaskSet.PASS)
        tasks.add(q.func, 1)
        tasks.add(q.func, 0)
        tasks.add(q.func, 2)
        res = tasks(q.func)
        res.wait(WAIT)

        fail = TaskFailure(
            'func', 'default', res.value[1].task_id, 'Exception: zero fail!')
        eq_(res.value, [1, fail, 2])


@example
def task_namespaces(url):
    state = set()
    __name__ = 'module.path'

    ts = TaskSpace(__name__)

    @ts.task
    def foo():
        state.add('foo')

    @ts.task
    def bar(arg):
        state.add(arg)

    broker = get_broker(url)
    broker.expose(ts)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url, target='module.path')

        q.foo()
        q.bar(1)

        eventually((lambda:state), {'foo', 1})


@example
def more_namespaces(url):
    state = set()

    foo = TaskSpace('foo')
    bar = TaskSpace('foo.bar')
    baz = TaskSpace('foo.bar.baz')

    @foo.task
    def join(arg):
        state.add('foo-join %s' % arg)

    @bar.task
    def kick(arg):
        state.add('bar-kick %s' % arg)

    @baz.task
    def join(arg):
        state.add('baz-join %s' % arg)

    @baz.task
    def kick(arg):
        state.add('baz-kick %s' % arg)

    broker = get_broker(url)
    broker.expose(foo)
    broker.expose(bar)
    broker.expose(baz)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        q.foo.join(1)
        q.foo.bar.kick(2)
        q.foo.bar.baz.join(3)
        q.foo.bar.baz.kick(4)

        eventually((lambda:state), {
            'foo-join 1',
            'bar-kick 2',
            'baz-join 3',
            'baz-kick 4',
        })
