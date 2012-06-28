# PyMQ implementation
from __future__ import absolute_import
from urlparse import urlparse
from pymq.core import DEFAULT, Broker, Task, TaskSet, TaskFailure, TaskSpace
from pymq.memory import MemoryQueue, MemoryResults
from pymq.redis import RedisQueue, RedisResults

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

def Queue(url, namespace='', name=DEFAULT):
    """Get a queue object for invoking remote tasks

    :param url: URL of the task queue.
    :param namespace: Task namespace (similar to a python module). Must match
        the namespace in which the remote callable was exposed.
    :param name: The name of the queue on which tasks should be invoked.
        Queued tasks will be invoked iff there is a worker listening on this
        named queue. Default value: 'default'.
    :returns: A Queue object.
    """
    broker = get_broker(url)
    return broker.queue(namespace, name=name)
