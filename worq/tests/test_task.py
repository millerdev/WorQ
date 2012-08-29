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

from worq import get_broker, queue, Task, TaskSet, TaskFailure, TaskSpace
from worq.tests.util import (assert_raises, eq_, eventually, thread_worker,
    with_urls)

WAIT = 60 # default wait time (1 minute)

def test_empty_TaskSet_with_identity_task():
    tasks = TaskSet()
    res = tasks()
    assert res, repr(res)
    eq_(res.value, [])
    eq_(repr(res), '<DeferredResult worq.task.identity [:] success>')

@with_urls
def test_empty_TaskSet(url):
    broker = get_broker(url)
    broker.expose(len)
    with thread_worker(broker):

        q = queue(url)

        tasks = TaskSet(result_timeout=60)
        res = tasks(q.len)
        assert res.wait(WAIT), repr(res)
        eq_(res.value, 0)
        assert ' len ' in repr(res), repr(res)

@with_urls
def test_TaskSet_on_error_FAIL(url):

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        tasks = TaskSet(result_timeout=WAIT)
        tasks.add(q.func, 1)
        tasks.add(q.func, 0)
        tasks.add(q.func, 2)
        res = tasks(q.func)
        res.wait(timeout=WAIT)

        with assert_raises(TaskFailure,
                'func [default:%s] subtask(s) failed' % res.id):
            res.value

@with_urls
def test_DeferredResult_wait_with_status_update(url):

    def func(arg, update_status=None):
        update_status('running')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        task = Task(q.func, result_status=True, result_timeout=WAIT)
        res = task(1)
        res.wait(timeout=WAIT)

        eq_(res.value, 1)
