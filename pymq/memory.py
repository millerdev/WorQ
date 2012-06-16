from pymq.broker import BaseBroker, Result, resultset
from weakref import WeakValueDictionary, WeakKeyDictionary

MEM_BROKERS = WeakValueDictionary()

class MemoryBroker(BaseBroker):

    @classmethod
    def factory(cls, data):
        url = data.geturl()
        broker = MEM_BROKERS.get(url)
        if broker is None:
            broker = MEM_BROKERS[url] = cls(data)
        return broker

    def __init__(self, *args, **kw):
        super(MemoryBroker, self).__init__(*args, **kw)
        self.queues = {}
        self.tasks = WeakValueDictionary()
        self.results = WeakKeyDictionary()

    def subscribe(self, worker, queues):
        for queue in queues:
            self.queues[queue] = worker

    def push_task(self, queue, blob):
        self.invoke(queue, blob)

    def deferred_result(self, task_id):
        result = super(MemoryBroker, self).deferred_result(task_id)
        self.tasks[task_id] = result
        return result

    def set_result_blob(self, task_id, blob):
        result_obj = self.tasks[task_id]
        self.results[result_obj] = blob

    def pop_result_blob(self, task_id):
        result_obj = self.tasks[task_id]
        return self.results.pop(result_obj, None)
