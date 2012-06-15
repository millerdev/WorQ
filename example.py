# PyMQ examples
from pymq import Broker, TaskSet

def simple(url):
    # -- tasks.py --
    def func(arg):
        return arg

    #if __name__ == '__main__':
    cn = Broker(url)
    w = cn.worker()
    w.publish(func)
    w.start()

    # -- task-invoking code --
    cn = Broker(url)
    default = cn.queue()
    res = default.func('arg')
    assert res.completed
    assert res.value == 'arg', repr(res)

def method_publishing(url):
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

    #if __name__ == '__main__':
    db = Database()
    obj = TaskObj(db)
    cn = Broker(url)
    w = cn.worker('obj')
    w.publish(obj.update_value)
    w.start()

    # -- task-invoking code --
    cn = Broker(url)
    obj = cn.queue('obj')
    res = obj.update_value(2)
    assert res.completed
    assert res.value is None, repr(res)

def taskset(url):
    # -- tasks.py --
    def func(arg):
        return arg
    def get_number(num):
        return num

    #if __name__ == '__main__':
    cn = Broker(url)
    w = cn.worker()
    w.publish(sum)
    w.publish(get_number)
    w.start()

    # -- task-invoking code --
    cn = Broker(url)
    q = cn.queue()
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
    url = 'null://'
    simple(url)
    method_publishing(url)
    taskset(url)

def test_redis():
    url = 'redis://localhost:6379/0'
    simple(url)
    method_publishing(url)
    taskset(url)
