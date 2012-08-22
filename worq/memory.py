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

"""In-memory message queue and result store.

MemoryQueue is not suitable for long-running processes that use TaskSets.
"""
import logging
from worq.core import AbstractMessageQueue, DEFAULT
from Queue import Queue, Empty
from weakref import WeakValueDictionary, WeakKeyDictionary

log = logging.getLogger(__name__)

_REFS = WeakValueDictionary()

class MemoryQueue(AbstractMessageQueue):
    """Simple in-memory message queue implementation

    Does not support named queues.
    """

    @classmethod
    def factory(cls, url, name=DEFAULT, *args, **kw):
        obj = _REFS.get((url, name))
        if obj is None:
            obj = _REFS[(url, name)] = cls(url, name, *args, **kw)
        return obj

    def __init__(self, *args, **kw):
        super(MemoryQueue, self).__init__(*args, **kw)
        self.queue = Queue()
        self.results_by_task = WeakValueDictionary()
        self.tasksets = {}

    def get(self, timeout=None):
        # TODO handle Empty, return None
        return self.queue.get(timeout=timeout)

    def __iter__(self):
        while True:
            yield self.get()

    def enqueue_task(self, message):
        self.queue.put(message)

    def discard_pending(self):
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break

    def deferred_result(self, task_id):
        result = self.results_by_task.get(task_id)
        if result is None:
            result = super(MemoryQueue, self).deferred_result(task_id)
            self.results_by_task[task_id] = result
            result.__status = None
            result.__result = Queue()
        return result

    def set_result(self, task_id, message, timeout):
        result_obj = self.results_by_task[task_id]
        result_obj.__result.put(message)

    def pop_result(self, task_id, timeout):
        result_obj = self.results_by_task[task_id]
        try:
            if timeout == 0:
                result = result_obj.__result.get_nowait()
            else:
                result = result_obj.__result.get(timeout=timeout)
        except Empty:
            result = None
        return result

    def update(self, taskset_id, num, message, timeout):
        """not thread-safe and leaks memory if a taskset is not completed"""
        value = self.tasksets.setdefault(taskset_id, [])
        value.append(message)
        if len(value) == num:
            return self.tasksets.pop(taskset_id)
