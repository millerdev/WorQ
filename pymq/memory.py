"""In-memory message queue and result store, normally used for testing."""
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

    def __iter__(self):
        while True:
            yield self.queue.get()

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
        self.results = WeakKeyDictionary()
        self.statuses = WeakKeyDictionary()
        self.tasksets = {}

    def deferred_result(self, task_id):
        result = self.results_by_task.get(task_id)
        if result is None:
            result = super(MemoryResults, self).deferred_result(task_id)
            self.results_by_task[task_id] = result
        return result

    def set_result(self, task_id, message, timeout):
        result_obj = self.results_by_task[task_id]
        self.results[result_obj] = message

    def pop_result(self, task_id):
        result_obj = self.results_by_task[task_id]
        return self.results.pop(result_obj, None)

    def set_status(self, task_id, message, timeout):
        result_obj = self.results_by_task.get(task_id)
        if result_obj is not None:
            self.statuses[result_obj] = message

    def pop_status(self, task_id):
        result_obj = self.results_by_task[task_id]
        return self.statuses.pop(result_obj)

    def update(self, taskset_id, num, message, timeout):
        # not thread-safe and leaks memory if a taskset is not completed
        value = self.tasksets.setdefault(taskset_id, [])
        value.append(message)
        if len(value) == num:
            return self.tasksets.pop(taskset_id)
