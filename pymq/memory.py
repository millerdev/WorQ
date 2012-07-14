"""In-memory message queue and result store.

MemoryResults is not suitable for long-running processes that use TaskSets.
"""
import logging
from pymq.core import AbstractMessageQueue, AbstractResultStore, DEFAULT
from Queue import Queue, Empty
from weakref import WeakValueDictionary, WeakKeyDictionary

log = logging.getLogger(__name__)

_REFS = WeakValueDictionary()

def _get_ref(key, cls, url, *args, **kw):
    obj = _REFS.get((url, key))
    if obj is None:
        obj = _REFS[(url, key)] = cls(url, *args, **kw)
    return obj

class MemoryQueue(AbstractMessageQueue):
    """Simple in-memory message queue implementation

    Does not support named queues.
    """

    @classmethod
    def factory(*args, **kw):
        return _get_ref('queue', *args, **kw)

    def __init__(self, *args, **kw):
        super(MemoryQueue, self).__init__(*args, **kw)
        if self.queues != [DEFAULT]:
            log.warn('MemoryQueue does not support named queues')
        self.queue = Queue()

    def get(self, timeout=None):
        # TODO handle Empty, return None
        return self.queue.get(timeout=timeout)

    def __iter__(self):
        while True:
            yield self.get()

    def enqueue_task(self, queue, message):
        self.queue.put((queue, message))

    def discard_pending(self):
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break


class MemoryResults(AbstractResultStore):
    # this result store is not thread-safe

    @classmethod
    def factory(*args, **kw):
        return _get_ref('results', *args, **kw)

    def __init__(self, *args, **kw):
        super(MemoryResults, self).__init__(*args, **kw)
        self.results_by_task = WeakValueDictionary()
        self.tasksets = {}

    def deferred_result(self, task_id):
        result = self.results_by_task.get(task_id)
        if result is None:
            result = super(MemoryResults, self).deferred_result(task_id)
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

    def set_status(self, task_id, message, timeout):
        result_obj = self.results_by_task.get(task_id)
        if result_obj is not None:
            result_obj.__status = message

    def pop_status(self, task_id):
        result_obj = self.results_by_task[task_id]
        return result_obj.__status

    def update(self, taskset_id, num, message, timeout):
        """not thread-safe and leaks memory if a taskset is not completed"""
        value = self.tasksets.setdefault(taskset_id, [])
        value.append(message)
        if len(value) == num:
            return self.tasksets.pop(taskset_id)
