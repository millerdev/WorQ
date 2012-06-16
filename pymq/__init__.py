# PyMQ implementation
from __future__ import absolute_import
from urlparse import urlparse
from pymq.core import DEFAULT, TaskSet
from pymq.memory import MemoryBroker
from pymq.redis import RedisBroker

BROKER_REGISTRY = {
    'memory': MemoryBroker.factory,
    'redis': RedisBroker,
}

def Broker(url, *queues):
    """Create a new broker

    :param url: The broker URL (example: "memory://").
    :param *queues: One or more queue names on which to publish functions.
    """
    data = urlparse(url)
    try:
        make_broker = BROKER_REGISTRY[data.scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    return make_broker(data, *queues)

def Queue(url, name=DEFAULT):
    """Get a queue object for invoking remote tasks

    :param url: URL of the task queue.
    :param name: The name of the queue on which tasks should be invoked.
    :returns: A Queue object.
    """
    broker = Broker(url)
    return broker.queue(name)
