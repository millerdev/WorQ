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

import worq.const as const
from worq import get_broker, get_queue, Task, TaskFailure, TaskSpace
from worq.tests.test_examples import example
from worq.tests.util import (assert_raises, eq_, eventually,
    thread_worker, TimeoutLock, WAIT)

@example
def simple(url):
    state = []

    def func(arg):
        state.append(arg)

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

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
        q = get_queue(url)
        q.update_value(2)
        res = Task(q.update_value, ignore_result=True)(2)

        eventually((lambda:db.value), 2)
        eq_(res, None) # verify that we did not get a deferred result

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
        q = get_queue(url, 'name')
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
        q = get_queue(url)

        res = q.func('arg')

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

    def func(arg):
        lock.acquire()
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker, lock):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        res = q.func('arg')

        eventually((lambda:res.status), const.ENQUEUED)
        eq_(repr(res), "<Deferred func [default:%s] enqueued>" % res.id)

        lock.release()
        eventually((lambda:res.status), const.PROCESSING)
        eq_(repr(res), "<Deferred func [default:%s] processing>" % res.id)

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
        q = get_queue(url)

        res = q.func('arg')

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
        q = get_queue(url)

        res = q.func('arg')
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
        q = get_queue(url)

        res = q.func('arg')
        completed = res.wait(WAIT)

        assert completed, repr(res)
        eq_(repr(res), '<Deferred func [default:%s] failed>' % res.id)
        with assert_raises(TaskFailure,
                'func [default:%s] Exception: fail!' % res.id):
            res.value


@example
def task_with_deferred_arguments(url):

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    broker.expose(sum)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        res = q.sum([
            q.func(1),
            q.func(2),
            q.func(3),
        ])

        assert res.wait(WAIT), repr(res)
        eq_(res.value, 6)


@example
def more_deferred_arguments(url):
    from operator import add

    def func(arg):
        return arg

    broker = get_broker(url)
    broker.expose(func)
    broker.expose(sum)
    broker.expose(add)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        sum_123 = q.sum([
            q.func(1),
            q.func(2),
            q.func(3),
        ])

        sum_1234 = q.add(sum_123, q.func(4))

        assert sum_1234.wait(WAIT), repr(res)
        eq_(sum_1234.value, 10)


@example
def dependency_graph(url):
    """Dependency graph
                         |
            _____________|_____________
           /             |             \
          / \           / \           / \
         /   \         /   \         /   \
      left   right  left   right  left   right
         \   /         \   /         \   /
          \ /           \ /           \ /
         catch         catch         catch
            \            |            /
             \___________|___________/
                         |
                      combine
    """
    ts = TaskSpace()

    @ts.task
    def left(num):
        return ('left', num)

    @ts.task
    def right(num):
        return ('right', num)

    @ts.task
    def catch(left, right, num):
        return [num, left, right]

    @ts.task
    def combine(items):
        return {i[0]: i[1:] for i in items}

    broker = get_broker(url)
    broker.expose(ts)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        catches = []
        for num in [1, 2, 3]:
            left = q.left(num)
            right = q.right(num)

            catch = q.catch(left, right, num)

            catches.append(catch)

        res = q.combine(catches)
        assert res.wait(WAIT), repr(res)

        eq_(res.value, {
            1: [('left', 1), ('right', 1)],
            2: [('left', 2), ('right', 2)],
            3: [('left', 3), ('right', 3)],
        })


# TODO test pass completed deferred result to task (should work)


@example
def task_with_failed_deferred_arguments(url):
    """TaskFailure can be passed to the final task.

    By default, a task fails if any of its deferred arguments fail. However,
    setting the `on_error=Task.PASS` option will cause a TaskFailure to be
    passed as the result of any task that fails.
    """

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        task = Task(q.func, on_error=Task.PASS)
        res = task([
            q.func(1),
            q.func(0),
            q.func(2),
        ])
        res.wait(timeout=WAIT)

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
        q = get_queue(url, target='module.path')

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
        q = get_queue(url)

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
