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
from worq import get_broker, get_queue, Task
from worq.task import Task, TaskFailure, TaskExpired
from worq.tests.util import (assert_raises, eq_, eventually, thread_worker,
    with_urls, TimeoutLock, WAIT)


@with_urls
def test_Broker_task_failed(url):
    lock = TimeoutLock(locked=True)
    def func():
        lock.acquire()
    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):
        q = get_queue(url)

        res = q.func()
        broker.task_failed(res)
        assert res.wait(timeout=WAIT), repr(res)
        lock.release()

        with assert_raises(TaskExpired):
            res.value


@with_urls
def test_Broker_duplicate_task_id_string(url):
    Broker_duplicate_task_id(url, 'int')

@with_urls
def test_Broker_duplicate_task_id_function(url):
    Broker_duplicate_task_id(url, lambda arg: type(arg).__name__)

def Broker_duplicate_task_id(url, identifier):
    lock = TimeoutLock(locked=True)
    state = []

    def func(arg):
        lock.acquire()
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker, lock):
        q = get_queue(url)

        task = Task(q.func, id=identifier)
        res = task(1)

        eventually((lambda:res.status), const.ENQUEUED)
        msg = 'func [default:int] cannot enqueue task with duplicate id'
        with assert_raises(TaskFailure, msg):
            task(2)

        lock.release()
        eventually((lambda:res.status), const.PROCESSING)
        msg = 'func [default:int] cannot enqueue task with duplicate id'
        with assert_raises(TaskFailure, msg):
            task(3)

        lock.release()
        assert res.wait(timeout=WAIT), repr(res)
        eq_(res.value, 1)

        res = task(4)
        eventually((lambda:res.status), const.ENQUEUED)
        lock.release()
        eventually((lambda:res.status), const.PROCESSING)
        lock.release()
        assert res.wait(timeout=WAIT), repr(res)
        eq_(res.value, 4)
