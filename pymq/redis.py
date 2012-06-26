"""Redis queue broker."""
from __future__ import absolute_import
import redis
from pymq.core import AbstractBroker

QUEUE_PATTERN = 'pymq:queue:%s'
RESULT_PATTERN = 'pymq:result:%s'
TASKSET_PATTERN = 'pymq:taskset:%s'


class RedisBroker(AbstractBroker):
    """Redis broker implementation

    In addition to the normal broker arguments, `__init__` accepts a
    `redis_factory` argument, which can be used to customize redis
    connection instantiation.
    """

    def __init__(self, *args, **kw):
        redis_factory = kw.pop('redis_factory', redis.StrictRedis)
        super(RedisBroker, self).__init__(*args, **kw)
        db = int(self.path.lstrip('/'))
        self.redis = redis_factory(self.host, self.port, db=db)

    def subscribe(self, queues):
        queue_names = [QUEUE_PATTERN % q for q in queues]
        queue_trim = len(QUEUE_PATTERN % '')
        while True:
            queue, message = self.redis.blpop(*queue_names)
            self.invoke(queue[queue_trim:], message)

    def enqueue_task(self, queue, message):
        key = QUEUE_PATTERN % queue
        self.redis.rpush(key, message)
        return key

    def set_result_message(self, task_id, message, timeout):
        key = RESULT_PATTERN % task_id
        self.redis.setex(key, timeout, message)

    def pop_result_message(self, task_id):
        key = RESULT_PATTERN % task_id
        pipe = self.redis.pipeline()
        pipe.get(key)
        pipe.delete(key)
        return pipe.execute()[0]

    def update_results(self, taskset_id, num_tasks, message, timeout):
        key = TASKSET_PATTERN % taskset_id
        num = self.redis.rpush(key, message)
        if num == num_tasks:
            pipe = self.redis.pipeline()
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            return pipe.execute()[0]
        else:
            self.redis.expire(key, timeout)
