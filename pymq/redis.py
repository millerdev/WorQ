from __future__ import absolute_import
import redis
from pymq.broker import BaseBroker

QUEUE_PATTERN = 'pymq:queue:%s'
RESULT_PATTERN = 'pymq:result:%s'


class RedisBroker(BaseBroker):

    def __init__(self, *args, **kw):
        super(RedisBroker, self).__init__(*args, **kw)
        db = int(self.path.lstrip('/'))
        self.redis = redis.Redis(self.host, self.port, db=db)

    def subscribe(self, worker, queues):
        queue_names = [QUEUE_PATTERN % q for q in queues]
        while True:
            msg = self.redis.blpop(*queue_names)
            self.invoke(msg[1])

    def push_task(self, queue, blob):
        key = QUEUE_PATTERN % queue
        self.redis.rpush(key, blob)
        return key

    def add_result(self, task_id, blob):
        key = RESULT_PATTERN % task_id
        self.redis.sadd(key, blob)

    def get_results(self, task_id):
        key = RESULT_PATTERN % task_id
        return self.redis.smembers(key)
