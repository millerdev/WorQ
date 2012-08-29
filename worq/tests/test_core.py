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

from worq import get_broker, queue, Task
from worq.task import TaskExpired
from worq.tests.util import (assert_raises, eq_, eventually, thread_worker,
    with_urls, TimeoutLock)

WAIT = 60 # default wait time (1 minute)


@with_urls
def test_Broker_task_failed(url):
    lock = TimeoutLock(locked=True)
    def func():
        lock.acquire()
    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):
        q = queue(url)

        res = Task(q.func, result_timeout=WAIT)()
        broker.task_failed(res)
        assert res.wait(timeout=WAIT), repr(res)
        lock.release()

        with assert_raises(TaskExpired):
            res.value
