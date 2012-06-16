# PyMQ examples
from threading import Thread
from pymq import Broker, TaskSet

def simple(url, run):
    # -- tasks.py --
    def func(arg):
        return arg

    #if __name__ == '__main__':
    broker = Broker(url)
    w = broker.worker()
    w.publish(func)
    def block():
        w.start()
    run(block)

    # -- task-invoking code --
    broker = Broker(url)
    default = broker.queue()
    res = default.func('arg')
    assert res.completed
    assert res.value == 'arg', repr(res)

def method_publishing(url, run):
    # -- tasks.py --
    class Database(object):
        """stateful storage"""
        def update(self, value):
            self.value = value
    class TaskObj(object):
        """object with task definitions"""
        def __init__(self, db):
            self.db = db
        def update_value(self, value):
            self.db.update(value)

    db = Database()
    obj = TaskObj(db)
    broker = Broker(url)
    w = broker.worker()
    w.publish(obj.update_value)
    def block():
        w.start()
    run(block)

    # -- task-invoking code --
    broker = Broker(url)
    obj = broker.queue()
    res = obj.update_value(2)
    assert res.completed
    assert res.value is None, repr(res)

def taskset(url, run):
    # -- tasks.py --
    def func(arg):
        return arg
    def get_number(num):
        return num

    #if __name__ == '__main__':
    broker = Broker(url)
    w = broker.worker()
    w.publish(sum)
    w.publish(get_number)
    def block():
        w.start()
    run(block)

    # -- task-invoking code --
    broker = Broker(url)
    q = broker.queue()
    tasks = TaskSet()
    for n in [1, 2, 3]:
        tasks.add(q.get_number, n)
    res = tasks()
    assert res.completed
    assert res.value == {1, 2, 3}, repr(res)

    res = tasks(q.sum)
    assert res.completed
    assert res.value == 6, repr(res)

def test():
    url = 'memory://'
    broker = Broker(url) # keep a reference so we always get the same one
    def run(block):
        block()

    simple(url, run)
    method_publishing(url, run)
    taskset(url, run)

#def test_redis():
#    url = 'redis://localhost:6379/0'
#    def run(block):
#        t = Thread(target=block)
#        t.daemon = True
#        t.start()
#
#    simple(url, run)
#    method_publishing(url, run)
#    taskset(url, run)
