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

"""In-memory message queue and result store."""
import logging
from Queue import Queue, Empty
from threading import Lock
from weakref import WeakValueDictionary

import worq.const as const
from worq.core import AbstractMessageQueue

log = logging.getLogger(__name__)

_REFS = WeakValueDictionary()

class MemoryQueue(AbstractMessageQueue):
    """Simple in-memory message queue implementation

    Does not support named queues.
    """

    @classmethod
    def factory(cls, url, name=const.DEFAULT, *args, **kw):
        obj = _REFS.get((url, name))
        if obj is None:
            obj = _REFS[(url, name)] = cls(url, name, *args, **kw)
        return obj

    def __init__(self, *args, **kw):
        super(MemoryQueue, self).__init__(*args, **kw)
        self.queue = Queue()
        self.results_by_task = WeakValueDictionary()

    def enqueue_task(self, task_id, message, result):
        # FIXME given result may not be the real result object.
        # Should this method return the real one?
        self.queue.put((task_id, message))
        if result is not None:
            result = self.results_by_task.setdefault(task_id, result)
            result.__status = const.ENQUEUED
            if not hasattr(result, '_%s__result' % type(self).__name__):
                result.__result = Queue()

    def get(self, timeout=None):
        try:
            task = self.queue.get(timeout=timeout)
        except Empty:
            task = None
        if task is not None:
            result_obj = self.results_by_task.get(task[0])
            if result_obj is not None:
                result_obj.__status = const.PROCESSING
        return task

    def discard_pending(self):
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break

    def set_task_timeout(self, task_id, timeout):
        pass

    def set_status(self, task_id, message):
        result_obj = self.results_by_task.get(task_id)
        if result_obj is not None:
            result_obj.__status = message

    def get_status(self, task_id):
        result_obj = self.results_by_task.get(task_id)
        return None if result_obj is None else result_obj.__status

    def set_result(self, task_id, message, timeout):
        result_obj = self.results_by_task.get(task_id)
        if result_obj is not None:
            result_obj.__result.put(message)

    def pop_result(self, task_id, timeout):
        result_obj = self.results_by_task.get(task_id)
        if result_obj is None:
            return const.TASK_EXPIRED
        try:
            if timeout == 0:
                result = result_obj.__result.get_nowait()
            else:
                result = result_obj.__result.get(timeout=timeout)
        except Empty:
            result = None
        return result

    def discard_result(self, task_id, task_expired_token):
        result_obj = self.results_by_task.pop(task_id)
        if result_obj is not None:
            result_obj.__result.put(task_expired_token)

    def init_taskset(self, taskset_id, task_message, result):
        if result is not None:
            self.results_by_task[taskset_id] = result
            result.__result = Queue() # final result queue
            result.__task = task_message
            result.__subsets = [] # child taskset result references
            result.__results = [] # taskset results
            result.__lock = Lock()
            if 'taskset_id' in result.task.options:
                # bind to parent taskset to prevent result loss due to GC
                parent_id = result.task.options['taskset_id']
                self.results_by_task[parent_id].__subsets.append(result)

    def update_taskset(self, taskset_id, num, message, timeout):
        result_obj = self.results_by_task.get(taskset_id)
        if result_obj is not None:
            with result_obj.__lock:
                value = result_obj.__results
                value.append(message)
                if len(value) == num:
                    return result_obj.__task, value
