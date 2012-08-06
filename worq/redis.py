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
from urlparse import urlparse
from worq.core import AbstractMessageQueue, AbstractResultStore, DAY

log = logging.getLogger(__name__)

QUEUE_PATTERN = 'worq:queue:%s'
RESULT_PATTERN = 'worq:result:%s'
STATUS_PATTERN = 'worq:status:%s'
TASKSET_PATTERN = 'worq:taskset:%s'


class RedisBackendMixin(object):
    """Connect to redis on __init__

    In addition to the normal arguments accepted by `AbstractMessageQueue`,
    `__init__` accepts a `redis_factory` argument, which can be used to
    customize redis connection instantiation.

    NOTE this mixin depends on Python's new-style-class method resolution order.
    """

    def __init__(self, url, *args, **kw):
        redis_factory = kw.pop('redis_factory', redis.StrictRedis)
        super(RedisBackendMixin, self).__init__(url, *args, **kw)
        urlobj = urlparse(url)
        if ':' in urlobj.netloc:
            host, port = urlobj.netloc.rsplit(':', 1)
        else:
            host, port = urlobj.netloc, 6379
        db = int(urlobj.path.lstrip('/'))
        self.redis = redis_factory(host, int(port), db=db)


class RedisQueue(RedisBackendMixin, AbstractMessageQueue):
    """Redis message queue"""

    def __init__(self, *args, **kw):
        super(RedisQueue, self).__init__(*args, **kw)
        self.queue_keys = [QUEUE_PATTERN % q for q in self.queues]
        self.queue_trim = len(QUEUE_PATTERN % '')

    def get(self, timeout=0):
        item = self.redis.blpop(self.queue_keys, timeout=timeout)
        if item is None:
            return item
        queue, message = item
        return (queue[self.queue_trim:], message)

    def __iter__(self):
        while True:
            yield self.get()

    def enqueue_task(self, queue, message):
        key = QUEUE_PATTERN % queue
        self.redis.rpush(key, message)

    def discard_pending(self):
        self.redis.delete(*[QUEUE_PATTERN % q for q in self.queues])


class RedisResults(RedisBackendMixin, AbstractResultStore):
    """Redis result store"""

    def set_result(self, task_id, message, timeout):
        key = RESULT_PATTERN % task_id
        pipe = self.redis.pipeline()
        pipe.rpush(key, message)
        pipe.expire(key, timeout)
        pipe.execute()

    def pop_result(self, task_id, timeout):
        key = RESULT_PATTERN % task_id
        if timeout == 0:
            return self.redis.lpop(key)
        if timeout is None:
            timeout = 0
        result = self.redis.blpop([key], timeout=timeout)
        return result if result is None else result[1]

    def update(self, taskset_id, num_tasks, message, timeout):
        key = TASKSET_PATTERN % taskset_id
        num = self.redis.rpush(key, message)
        if num == num_tasks:
            pipe = self.redis.pipeline()
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            return pipe.execute()[0]
        else:
            self.redis.expire(key, timeout)
