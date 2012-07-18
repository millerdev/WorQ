from __future__ import absolute_import
from urlparse import urlparse
from worq.core import DEFAULT, Broker
from worq.task import Task, TaskSet, TaskFailure, TaskSpace
from worq.memory import MemoryQueue, MemoryResults
from worq.redis import RedisQueue, RedisResults

BROKER_REGISTRY = {
    'memory': (MemoryQueue.factory, MemoryResults.factory),
    'redis': (RedisQueue, RedisResults),
}

def get_broker(url, *queues):
    """Create a new broker

    :param url: Message queue and result store URL (this convenience function
        uses the same URL to construct both).
    :param *queues: One or more queue names on which to expose or invoke tasks.
    """
    url_scheme = urlparse(url).scheme
    try:
        make_queue, make_results = BROKER_REGISTRY[url_scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    message_queue = make_queue(url, queues)
    result_store = make_results(url)
    return Broker(message_queue, result_store)

def queue(url, queue=DEFAULT, target=''):
    """Get a queue object for invoking remote tasks

    :param url: URL of the task queue.
    :param queue: The name of the queue on which tasks should be invoked.
        Queued tasks will be invoked iff there is a worker listening on the
        named queue. Default value: 'default'.
    :param target: Task namespace (similar to a python module) or name
        (similar to a python function). Default to the root namespace ('').
    :returns: An instance of worq.task.Queue.
    """
    broker = get_broker(url)
    return broker.queue(queue, target)
