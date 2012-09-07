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

from __future__ import absolute_import
from urlparse import urlparse
from worq.core import DEFAULT, Broker
from worq.task import Task, TaskFailure, TaskSpace
from worq.queue.memory import TaskQueue as MemoryQueue
try:
    from worq.queue.redis import TaskQueue as RedisQueue
except ImportError:
    RedisQueue = None

__version__ = '1.0.1'

BROKER_REGISTRY = {'memory': MemoryQueue.factory}

if RedisQueue is not None:
    BROKER_REGISTRY['redis'] = RedisQueue

def get_broker(url, name=DEFAULT, *args, **kw):
    """Create a new broker

    :param url: Task queue URL.
    :param name: The name of the queue on which to expose or invoke tasks.
    :returns: An instance of ``worq.core.Broker``.
    """
    scheme = urlparse(url).scheme
    try:
        factory = BROKER_REGISTRY[scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    return Broker(factory(url, name, *args, **kw))

def get_queue(url, name=DEFAULT, target='', **options):
    """Get a queue for invoking remote tasks

    :param url: Task queue URL.
    :param name: The name of the queue on which tasks should be invoked.
        Queued tasks will be invoked iff there is a worker listening on the
        named queue.
    :param target: Task namespace (similar to a python module) or name
        (similar to a python function). Defaults to the root namespace.
    :param **options: Default task options for tasks created with the queue.
        These can be overridden with ``worq.task.Task``.
    :returns: An instance of ``worq.task.Queue``.
    """
    return get_broker(url, name).queue(target, **options)
