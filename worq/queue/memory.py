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

"""In-memory message queue and result store."""
import logging
from Queue import Queue, Empty
from threading import Lock
from weakref import WeakValueDictionary

import worq.const as const
from worq.core import AbstractTaskQueue

log = logging.getLogger(__name__)

_REFS = WeakValueDictionary()

class TaskQueue(AbstractTaskQueue):
    """Simple in-memory task queue implementation"""

    @classmethod
    def factory(cls, url, name=const.DEFAULT, *args, **kw):
        obj = _REFS.get((url, name))
        if obj is None:
            obj = _REFS[(url, name)] = cls(url, name, *args, **kw)
        return obj

    def __init__(self, *args, **kw):
        super(TaskQueue, self).__init__(*args, **kw)
        self.queue = Queue()
        self.results = WeakValueDictionary()
        self.result_lock = Lock()

    def _init_result(self, result, status, message):
        with self.result_lock:
            if result.id in self.results:
                return False
            self.results[result.id] = result
        result.__status = status
        result.__value = Queue()
        result.__task = message
        result.__args = {}
        result.__lock = Lock()
        result.__for = None
        return True

    def enqueue_task(self, result, message):
        if self._init_result(result, const.ENQUEUED, message):
            self.queue.put(result)
            return True
        return False

    def defer_task(self, result, message, args):
        if self._init_result(result, const.PENDING, message):
            results = self.results
            # keep references to results to prevent GC
            result.__refs = [results[arg] for arg in args]
            return True
        return False

    def undefer_task(self, task_id):
        result = self.results[task_id]
        self.queue.put(result)

    def get(self, timeout=None):
        try:
            result = self.queue.get(timeout=timeout)
        except Empty:
            return None
        result.__status = const.PROCESSING
        return result.id, result.__task

    def discard_pending(self):
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break
        with self.result_lock:
            self.results.clear()

    def reserve_argument(self, argument_id, deferred_id):
        result = self.results.get(argument_id)
        if result is None:
            return (False, None)
        with result.__lock:
            if result.__for is not None:
                return (False, None)
            result.__for = deferred_id
            try:
                message = result.__value.get_nowait()
            except Empty:
                message = None
            return (True, message)

    def set_argument(self, task_id, argument_id, message):
        result = self.results[task_id]
        with result.__lock:
            result.__args[argument_id] = message
            return len(result.__args) == len(result.__refs)

    def get_arguments(self, task_id):
        try:
            return self.results[task_id].__args
        except KeyError:
            return {}

    def set_task_timeout(self, task_id, timeout):
        pass

    def get_status(self, task_id):
        result = self.results.get(task_id)
        return None if result is None else result.__status

    def set_result(self, task_id, message, timeout):
        result = self.results.get(task_id)
        if result is not None:
            with result.__lock:
                result.__value.put(message)
                return result.__for

    def pop_result(self, task_id, timeout):
        result = self.results.get(task_id)
        if result is None:
            return const.TASK_EXPIRED
#        with result.__lock:
#            if result.__for is not None:
#                raise NotImplementedError
#                #return const.RESERVED
#            result.__for = task_id
        try:
            if timeout == 0:
                value = result.__value.get_nowait()
            else:
                value = result.__value.get(timeout=timeout)
        except Empty:
            value = None
        else:
            self.results.pop(task_id, None)
        return value

    def discard_result(self, task_id, task_expired_token):
        result = self.results.pop(task_id)
        if result is not None:
            result.__value.put(task_expired_token)
