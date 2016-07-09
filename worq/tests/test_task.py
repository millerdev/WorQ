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

from worq import get_broker, get_queue, Task, TaskFailure
from worq.tests.util import (assert_raises, eq_, eventually, thread_worker,
    with_urls, WAIT, TimeoutLock)


@with_urls
def test_Queue_len(url):
    lock = TimeoutLock(locked=True)

    def func(arg=None):
        pass
    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker, lock):
        q = get_queue(url)
        eq_(len(q), 0)

        r0 = q.func()
        eq_(len(q), 1)

        r1 = q.func()
        r2 = q.func(r1)
        eq_(len(q), 3)

        eventually((lambda: lock.locked), True)
        lock.release()
        assert r0.wait(timeout=WAIT), repr(r0)
        eq_(len(q), 2)

        eventually((lambda: lock.locked), True)
        lock.release()
        eventually((lambda: lock.locked), True)
        lock.release()
        assert r2.wait(timeout=WAIT), repr(r2)
        eq_(len(q), 0)


@with_urls
def test_clear_Queue(url):
    q = get_queue(url)
    eq_(len(q), 0)

    q.func()
    q.func()
    eq_(len(q), 2)

    del q[:]
    eq_(len(q), 0)

    msg = 'delitem is only valid with a full slice ([:])'
    with assert_raises(ValueError, msg=msg):
        del q[:2]


@with_urls
def test_Queue_default_options(url):
    def func(arg=3):
        if isinstance(arg, int) and arg < 2:
            raise ValueError('too low')
        return str(arg)
    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):
        q = get_queue(url, ignore_result=True)
        eq_(q.func(), None)

        q = get_queue(url, on_error=Task.PASS)
        rx = q.func(1)
        res = q.func(rx)
        assert res.wait(WAIT), repr(res)
        eq_(res.value, 'func [default:%s] ValueError: too low' % rx.id)


@with_urls
def test_completed_Deferred_as_argument(url):
    def func(arg):
        eq_(arg, 1)
        return arg
    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):
        q = get_queue(url)
        eq_(len(q), 0)

        r0 = q.func(1)
        assert r0.wait(timeout=WAIT), repr(r0)
        eq_(r0.value, 1)

        r1 = q.func(r0)
        assert r1.wait(timeout=WAIT), repr(r1)
        eq_(r0.value, 1)


@with_urls
def test_worker_interrupted(url):

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


@with_urls
def test_deferred_task_fail_on_error(url):

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = get_queue(url)

        res = q.func([q.func(1), q.func(0), q.func(2)])
        res.wait(timeout=WAIT)

        msg = 'func [default:%s] Exception: zero fail!' % res.task.args[0][1].id
        with assert_raises(TaskFailure, msg):
            res.value
