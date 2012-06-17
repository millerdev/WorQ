# PyMQ implementation
from __future__ import absolute_import
from urlparse import urlparse
from pymq.core import DEFAULT, TaskSet, TaskSpace
from pymq.memory import MemoryBroker
from pymq.redis import RedisBroker

BROKER_REGISTRY = {
    'memory': MemoryBroker.factory,
    'redis': RedisBroker,
}

def Broker(url, *queues, **kw):
    """Create a new broker

    :param url: The broker URL (example: "memory://").
    :param *queues: One or more queue names on which to publish functions.
    :param **kw: Keyword arguments to pass to the broker constructor.
    """
    data = urlparse(url)
    try:
        make_broker = BROKER_REGISTRY[data.scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    return make_broker(data, *queues, **kw)

def Queue(url, namespace='', name=DEFAULT):
    """Get a queue object for invoking remote tasks

    :param url: URL of the task queue.
    :param namespace: Task namespace (similar to a python module). Must match
        the namespace in which the remote callable was published.
    :param name: The name of the queue on which tasks should be invoked.
        Queued tasks will be invoked iff there is a worker listening on this
        named queue. Default value: 'default'.
    :returns: A Queue object.
    """
    broker = Broker(url)
    return broker.queue(namespace, name=name)
