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
from worq.core import AbstractMessageQueue, DAY

log = logging.getLogger(__name__)

QUEUE_PATTERN = 'worq:queue:%s'
RESULT_PATTERN = 'worq:result:%s:%s'
TASKSET_PATTERN = 'worq:taskset:%s:%s'


class RedisQueue(AbstractMessageQueue):
    """Redis message queue"""

    def __init__(self, url, *args, **kw):
        redis_factory = kw.pop('redis_factory', redis.StrictRedis)
        super(RedisQueue, self).__init__(url, *args, **kw)
        urlobj = urlparse(url)
        if ':' in urlobj.netloc:
            host, port = urlobj.netloc.rsplit(':', 1)
        else:
            host, port = urlobj.netloc, 6379
        db = int(urlobj.path.lstrip('/'))
        self.redis = redis_factory(host, int(port), db=db)
        self.queue_key = QUEUE_PATTERN % self.name

    def enqueue_task(self, message):
        self.redis.rpush(self.queue_key, message)

    def get(self, timeout=0):
        item = self.redis.blpop(self.queue_key, timeout=timeout)
        if item is None:
            return item
        return item[1]

    def discard_pending(self):
        self.redis.delete(self.queue_key)

    def set_result(self, task_id, message, timeout):
        key = RESULT_PATTERN % (self.name, task_id)
        pipe = self.redis.pipeline()
        pipe.rpush(key, message)
        pipe.expire(key, timeout)
        pipe.execute()

    def pop_result(self, task_id, timeout):
        key = RESULT_PATTERN % (self.name, task_id)
        if timeout == 0:
            return self.redis.lpop(key)
        if timeout is None:
            timeout = 0
        result = self.redis.blpop([key], timeout=timeout)
        return result if result is None else result[1]

    def update(self, taskset_id, num_tasks, message, timeout):
        key = TASKSET_PATTERN % (self.name, taskset_id)
        num = self.redis.rpush(key, message)
        if num == num_tasks:
            pipe = self.redis.pipeline()
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            return pipe.execute()[0]
        else:
            self.redis.expire(key, timeout)
