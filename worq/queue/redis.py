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

"""Redis message queue and result store."""
from __future__ import absolute_import
import logging
import redis
import time
import worq.const as const
from urlparse import urlparse
from worq.core import AbstractTaskQueue

log = logging.getLogger(__name__)

QUEUE_PATTERN = 'worq:queue:%s'
TASK_PATTERN = 'worq:task:%s:%s'
RESULT_PATTERN = 'worq:result:%s:%s'

# args (for task_id) is a hash: result_id -> value
# each deferred and enqueued task may have an args key
ARGS_PATTERN = 'worq:args:%s:%s'

# reserve result_id (key) for deferred task_id (string value)
# each deferred and enqueued task may have a reserved key
RESERVE_PATTERN = 'worq:reserved:%s:%s'


class TaskQueue(AbstractTaskQueue):
    """Redis task queue"""

    def __init__(self, url, name=const.DEFAULT, initial_result_timeout=60,
            redis_factory=redis.StrictRedis):
        super(TaskQueue, self).__init__(url, name)
        urlobj = urlparse(url)
        if ':' in urlobj.netloc:
            host, port = urlobj.netloc.rsplit(':', 1)
        else:
            host, port = urlobj.netloc, 6379
        db = int(urlobj.path.lstrip('/'))
        self.redis = redis_factory(host, int(port), db=db)
        self.queue_key = QUEUE_PATTERN % self.name
        self.initial_result_timeout = max(int(initial_result_timeout), 1)

    def ping(self):
        return self.redis.ping()

    def log_all_worq(self, show_expiring=False):
        """debugging helper"""
        for key in self.redis.keys('worq:*'):
            ttl = self.redis.ttl(key)
            if ttl < 0 or show_expiring:
                if key.startswith(('worq:task:', 'worq:args:')):
                    log.debug('%s %s %s', key, ttl, self.redis.hgetall(key))
                else:
                    log.debug('%s %s', key, ttl)

    def enqueue_task(self, result, message):
        task_key = TASK_PATTERN % (self.name, result.id)
        args_key = ARGS_PATTERN % (self.name, result.id)
        result_key = RESULT_PATTERN % (self.name, result.id)
        reserve_key = RESERVE_PATTERN % (self.name, result.id)
        with self.redis.pipeline() as pipe:
            try:
                pipe.watch(task_key)
                if pipe.exists(task_key):
                    return False
                pipe.multi()
                pipe.hmset(task_key, {
                    'task': message,
                    'status': const.ENQUEUED
                })
                pipe.delete(args_key, result_key, reserve_key)
                pipe.lpush(self.queue_key, result.id) # left push (head)
                pipe.execute()
                return True
            except redis.WatchError:
                return False

    def defer_task(self, result, message, args):
        task_key = TASK_PATTERN % (self.name, result.id)
        args_key = ARGS_PATTERN % (self.name, result.id)
        result_key = RESULT_PATTERN % (self.name, result.id)
        reserve_key = RESERVE_PATTERN % (self.name, result.id)
        with self.redis.pipeline() as pipe:
            try:
                pipe.watch(task_key)
                if pipe.exists(task_key):
                    return False
                pipe.multi()
                pipe.hmset(task_key, {
                    'task': message,
                    'status': const.PENDING,
                    'total_args': len(args),
                    #'args_ready': 0, # zero by default
                })
                pipe.delete(args_key, result_key, reserve_key)
                pipe.execute()
                return True
            except redis.WatchError:
                return False

    def undefer_task(self, task_id):
        self.redis.lpush(self.queue_key, task_id)

    def get(self, timeout=0):
        result_timeout = self.initial_result_timeout
        queue = self.queue_key
        end = time.time() + timeout
        while True:
            task_id = self.redis.brpoplpush(queue, queue, timeout=timeout)
            if task_id is None:
                return None # timeout

            task_key = TASK_PATTERN % (self.name, task_id)
            args_key = ARGS_PATTERN % (self.name, task_id)
            reserve_key = RESERVE_PATTERN % (self.name, task_id)
            with self.redis.pipeline() as pipe:
                pipe.hget(task_key, 'task')
                pipe.hset(task_key, 'status', const.PROCESSING)
                pipe.expire(task_key, result_timeout)
                pipe.expire(args_key, result_timeout)
                pipe.expire(reserve_key, result_timeout)
                pipe.lrem(queue, 1, task_id) # traverse head to tail
                cmd = pipe.execute()
                message, removed = cmd[0], cmd[-1]
            if removed:
                return (task_id, message)

            if timeout != 0:
                timeout = int(end - time.time())
                if timeout <= 0:
                    return None

    def size(self):
        tasks = self.redis.lrange(self.queue_key, 0, -1)
        num = len(tasks)
        while tasks:
            keys = (RESERVE_PATTERN % (self.name, t) for t in tasks)
            tasks = [t for t in self.redis.mget(keys) if t is not None]
            num += len(tasks)
        return num

    def discard_pending(self):
        with self.redis.pipeline() as pipe:
            pipe.lrange(self.queue_key, 0, -1)
            pipe.delete(self.queue_key)
            tasks = pipe.execute()[0]
        while tasks:
            reserved = []
            all_keys = []
            for task_id in tasks:
                all_keys.append(TASK_PATTERN % (self.name, task_id))
                all_keys.append(ARGS_PATTERN % (self.name, task_id))
                all_keys.append(RESULT_PATTERN % (self.name, task_id))
                reserve_key = RESERVE_PATTERN % (self.name, task_id)
                all_keys.append(reserve_key)
                reserved.append(reserve_key)
            with self.redis.pipeline() as pipe:
                pipe.mget(reserved)
                pipe.delete(*all_keys)
                tasks = [v for v in pipe.execute()[0] if v is not None]

    def reserve_argument(self, argument_id, deferred_id):
        reserve_key = RESERVE_PATTERN % (self.name, argument_id)
        result_key = RESULT_PATTERN % (self.name, argument_id)
        if self.redis.setnx(reserve_key, deferred_id):
            value = self.redis.lpop(result_key)
            if value is not None:
                self.redis.delete(
                    TASK_PATTERN % (self.name, argument_id),
                    ARGS_PATTERN % (self.name, argument_id),
                    result_key,
                    reserve_key,
                )
            return (True, value)
        return (False, None)

    def set_argument(self, task_id, argument_id, message):
        task_key = TASK_PATTERN % (self.name, task_id)
        args_key = ARGS_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            pipe.hincrby(task_key, 'args_ready', 1)
            pipe.hget(task_key, 'total_args')
            pipe.hset(args_key, argument_id, message)
            ready, total, x = pipe.execute()
        total = int(total)
        assert isinstance(ready, (int, long)), repr(ready)
        assert ready > 0, ready
        assert total > 0, total
        return ready == total

    def get_arguments(self, task_id):
        args_key = ARGS_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            pipe.hgetall(args_key)
            pipe.delete(args_key)
            return pipe.execute()[0] or {}

    def set_task_timeout(self, task_id, timeout):
        def set_timeout(task_id, args=True, timeout=max(int(timeout), 1)):
            task_key = TASK_PATTERN % (self.name, task_id)
            reserve_key = RESERVE_PATTERN % (self.name, task_id)
            with self.redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(reserve_key)
                        deferred_id = pipe.get(reserve_key)
                        pipe.multi()
                        pipe.expire(task_key, timeout)
                        if args:
                            args_key = ARGS_PATTERN % (self.name, task_id)
                            pipe.expire(args_key, timeout)
                            pipe.expire(reserve_key, timeout)
                        pipe.execute()
                        return deferred_id
                    except redis.WatchError:
                        continue
        # Do not expire arguments of the first task since they have
        # already been deleted. However, all subsequent deferred tasks'
        # argument timeouts must be set.
        deferred_id = set_timeout(task_id, False)
        while deferred_id is not None:
            deferred_id = set_timeout(deferred_id)

    def get_status(self, task_id):
        key = TASK_PATTERN % (self.name, task_id)
        return self.redis.hget(key, 'status')

    def set_result(self, task_id, message, timeout):
        timeout = max(int(timeout), 1)
        task_key = TASK_PATTERN % (self.name, task_id)
        args_key = ARGS_PATTERN % (self.name, task_id)
        result_key = RESULT_PATTERN % (self.name, task_id)
        reserve_key = RESERVE_PATTERN % (self.name, task_id)
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(reserve_key)
                    deferred_id = pipe.get(reserve_key)
                    pipe.multi()
                    if deferred_id is None:
                        pipe.hset(task_key, 'status', const.COMPLETED)
                        pipe.expire(task_key, timeout)
                        pipe.rpush(result_key, message)
                        pipe.expire(result_key, timeout)
                    else:
                        pipe.delete(task_key, result_key)
                    pipe.delete(args_key, reserve_key)
                    pipe.execute()
                    return deferred_id
                except redis.WatchError:
                    continue

    def pop_result(self, task_id, timeout):
        result_key = RESULT_PATTERN % (self.name, task_id)
        if timeout == 0:
            return self.redis.lpop(result_key)

        task_key = TASK_PATTERN % (self.name, task_id)
        end = None if timeout is None else time.time() + timeout
        while True:
            with self.redis.pipeline() as pipe:
                pipe.ttl(task_key)
                pipe.exists(task_key)
                timeout, exists = pipe.execute()
                if end:
                    timeout = min(timeout, int(end - time.time()))
            t = timeout
            if timeout <= 0:
                if not exists:
                    # task has expired (heartbeat stopped)
                    result = self.redis.lpop(result_key)
                    return const.TASK_EXPIRED if result is None else result
                # task is still enqueued (heartbeat not started yet)
                timeout = 1 if end else 5
            result = self.redis.blpop([result_key], timeout=timeout)
            if result is None:
                if end and time.time() > end:
                    return None # timeout
                continue
            self.redis.delete(task_key)
            return result[1]

    def discard_result(self, task_id, task_expired_token):
        while task_id is not None:
            reserve_key = RESERVE_PATTERN % (self.name, task_id)
            result_key = RESULT_PATTERN % (self.name, task_id)
            args_key = ARGS_PATTERN % (self.name, task_id)
            task_key = TASK_PATTERN % (self.name, task_id)
            with self.redis.pipeline() as pipe:
                pipe.get(reserve_key) # reserved for deferred task
                pipe.delete(task_key, args_key, reserve_key)
                pipe.rpush(result_key, task_expired_token)
                pipe.expire(result_key, 5)
                task_id = pipe.execute()[0]
