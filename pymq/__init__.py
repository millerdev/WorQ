# PyMQ implementation
from __future__ import absolute_import
from urlparse import urlparse
from pymq.broker import TaskSet
from pymq.memory import MemoryBroker
from pymq.redis import RedisBroker

BROKER_REGISTRY = {
    'memory': MemoryBroker.factory,
    'redis': RedisBroker,
}

def Broker(url, *queues):
    data = urlparse(url)
    try:
        make_broker = BROKER_REGISTRY[data.scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    return make_broker(data, *queues)
