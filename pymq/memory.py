from pymq.broker import BaseBroker, Result
from weakref import WeakValueDictionary, WeakKeyDictionary

MEM_BROKERS = WeakValueDictionary()

class MemoryBroker(BaseBroker):
    # this broker is not thread-safe

    @classmethod
    def factory(cls, data, *args, **kw):
        url = data.geturl()
        broker = MEM_BROKERS.get(url)
        if broker is None:
            broker = MEM_BROKERS[url] = cls(data, *args, **kw)
        return broker

    def __init__(self, *args, **kw):
        super(MemoryBroker, self).__init__(*args, **kw)
        self.taskresults = WeakValueDictionary()
        self.results = WeakKeyDictionary()
        self.taskset_results = {}

    def subscribe(self, queues):
        pass

    def push_task(self, queue, blob):
        self.invoke(queue, blob)

    def deferred_result(self, task_id):
        result = self.taskresults.get(task_id)
        if result is None:
            result = super(MemoryBroker, self).deferred_result(task_id)
            self.taskresults[task_id] = result
        return result

    def set_result_blob(self, task_id, blob, timeout):
        result_obj = self.taskresults[task_id]
        self.results[result_obj] = blob

    def pop_result_blob(self, task_id):
        result_obj = self.taskresults[task_id]
        return self.results.pop(result_obj, None)

    def update_results(self, taskset_id, num, blob, timeout):
        # not thread-safe
        value = self.taskset_results.setdefault(taskset_id, [])
        value.append(blob)
        if len(value) == num:
            return self.taskset_results.pop(taskset_id)
