from __future__ import absolute_import
import redis
from pymq.core import BaseBroker

QUEUE_PATTERN = 'pymq:queue:%s'
RESULT_PATTERN = 'pymq:result:%s'
TASKSET_PATTERN = 'pymq:taskset:%s'


class RedisBroker(BaseBroker):

    def __init__(self, *args, **kw):
        super(RedisBroker, self).__init__(*args, **kw)
        db = int(self.path.lstrip('/'))
        self.redis = redis.Redis(self.host, self.port, db=db)

    def subscribe(self, queues):
        queue_names = [QUEUE_PATTERN % q for q in queues]
        queue_trim = len(QUEUE_PATTERN % '')
        while True:
            queue, blob = self.redis.blpop(*queue_names)
            self.invoke(queue[queue_trim:], blob)

    def push_task(self, queue, blob):
        key = QUEUE_PATTERN % queue
        self.redis.rpush(key, blob)
        return key

    def set_result_blob(self, task_id, blob, timeout):
        key = RESULT_PATTERN % task_id
        self.redis.setex(key, blob, timeout)

    def pop_result_blob(self, task_id):
        key = RESULT_PATTERN % task_id
        pipe = self.redis.pipeline()
        pipe.get(key)
        pipe.delete(key)
        return pipe.execute()[0]

    def update_results(self, taskset_id, num_tasks, blob, timeout):
        key = TASKSET_PATTERN % taskset_id
        num = self.redis.rpush(key, blob)
        if num == num_tasks:
            pipe = self.redis.pipeline()
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            return pipe.execute()[0]
        else:
            self.redis.expire(key, timeout)
