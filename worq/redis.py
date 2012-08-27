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

"""Redis queue broker."""
from __future__ import absolute_import
import logging
import redis
import time
import worq.const as const
from urlparse import urlparse
from worq.core import AbstractMessageQueue

log = logging.getLogger(__name__)

QUEUE_PATTERN = 'worq:queue:%s'
TASK_PATTERN = 'worq:task:%s:%s'
RESULT_PATTERN = 'worq:result:%s:%s'
TASKSET_PATTERN = 'worq:taskset:%s:%s'


class RedisQueue(AbstractMessageQueue):
    """Redis message queue

    WARNING this implementation depends on all task ids being unique.
    Behavior is undefined if two different tasks are submitted with the
    same task id.
    """

    def __init__(self, url, name=const.DEFAULT, initial_result_timeout=60,
            redis_factory=redis.StrictRedis):
        super(RedisQueue, self).__init__(url, name)
        urlobj = urlparse(url)
        if ':' in urlobj.netloc:
            host, port = urlobj.netloc.rsplit(':', 1)
        else:
            host, port = urlobj.netloc, 6379
        db = int(urlobj.path.lstrip('/'))
        self.redis = redis_factory(host, int(port), db=db)
        self.queue_key = QUEUE_PATTERN % self.name
        self.initial_result_timeout = max(int(initial_result_timeout), 1)

    def enqueue_task(self, task_id, message, result):
        key = TASK_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            pipe.hmset(key, {'task': message, 'status': const.ENQUEUED})
            pipe.lpush(self.queue_key, task_id) # left push (head)
            pipe.execute()

    def get(self, timeout=0):
        queue = self.queue_key
        end = time.time() + timeout
        while True:
            task_id = self.redis.brpoplpush(queue, queue, timeout=timeout)
            if task_id is None:
                return task_id # timeout

            key = TASK_PATTERN % (self.name, task_id)
            with self.redis.pipeline() as pipe:
                pipe.hget(key, 'task')
                pipe.hset(key, 'status', const.PROCESSING)
                pipe.expire(key, self.initial_result_timeout)
                pipe.lrem(queue, 1, task_id) # traverse head to tail
                message, x, x, removed = pipe.execute()
            if removed:
                return (task_id, message)

            if timeout != 0:
                timeout = int(end - time.time())
                if timeout <= 0:
                    return None

    def discard_pending(self):
        with self.redis.pipeline() as pipe:
            pipe.lrange(self.queue_key, 0, -1)
            pipe.delete(self.queue_key)
            tasks = pipe.execute()[0]
        if tasks:
            self.redis.delete(*(TASK_PATTERN % (self.name, t) for t in tasks))

    def set_task_timeout(self, task_id, timeout):
        timeout = max(int(timeout), 1)
        with self.redis.pipeline() as pipe:
            pipe.expire(TASK_PATTERN % (self.name, task_id), timeout)
            pipe.execute()

    def set_status(self, task_id, message):
        key = TASK_PATTERN % (self.name, task_id)
        self.redis.hset(key, 'status', message)

    def get_status(self, task_id):
        key = TASK_PATTERN % (self.name, task_id)
        return self.redis.hget(key, 'status')

    def set_result(self, task_id, message, timeout):
        timeout = max(int(timeout), 1)
        key = RESULT_PATTERN % (self.name, task_id)
        task = TASK_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            pipe.hset(task, 'status', const.COMPLETED)
            pipe.expire(task, timeout)
            pipe.rpush(key, message)
            pipe.expire(key, timeout)
            pipe.execute()

    def pop_result(self, task_id, timeout):
        key = RESULT_PATTERN % (self.name, task_id)
        if timeout == 0:
            return self.redis.lpop(key)

        task = TASK_PATTERN % (self.name, task_id)
        end = None if timeout is None else time.time() + timeout
        while True:
            with self.redis.pipeline() as pipe:
                pipe.ttl(task)
                pipe.exists(task)
                timeout, exists = pipe.execute()
                if end:
                    timeout = min(timeout, int(end - time.time()))
            t = timeout
            if timeout <= 0:
                if not exists:
                    # task has expired (heartbeat stopped)
                    result = self.redis.lpop(key)
                    return const.TASK_EXPIRED if result is None else result
                # task is still enqueued (heartbeat not started yet)
                timeout = 1 if end else 5
            result = self.redis.blpop([key], timeout=timeout)
            if result is None:
                if end and time.time() > end:
                    return None # timeout
                continue
            self.redis.delete(task)
            return result[1]

    def discard_result(self, task_id, task_expired_token):
        key = RESULT_PATTERN % (self.name, task_id)
        task = TASK_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            pipe.delete(task)
            pipe.rpush(key, task_expired_token)
            pipe.expire(key, 5)
            pipe.execute()

    def init_taskset(self, taskset_id, result):
        # TODO taskset result persistence should update heartbeat
        key = TASK_PATTERN % (self.name, taskset_id)
        self.redis.hmset(key, {'status': const.PENDING})

    def update_taskset(self, taskset_id, num_tasks, message, timeout):
        key = TASKSET_PATTERN % (self.name, taskset_id)
        num = self.redis.rpush(key, message)
        if num == num_tasks:
            with self.redis.pipeline() as pipe:
                pipe.lrange(key, 0, -1)
                pipe.delete(key)
                return pipe.execute()[0]
        else:
            self.redis.expire(key, max(int(timeout), 1))
