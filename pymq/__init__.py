# PyMQ implementation
from __future__ import absolute_import
from urlparse import urlparse
from pymq.broker import TaskSet
from pymq.memory import MemoryBroker
from pymq.redis import RedisBroker

BROKERS = {
    'memory': MemoryBroker.factory,
    'redis': RedisBroker,
}

def Broker(url):
    data = urlparse(url)
    try:
        broker_factory = BROKERS[data.scheme]
    except KeyError:
        raise ValueError('invalid broker URL: %s' % url)
    return broker_factory(data)
