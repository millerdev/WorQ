# PyMQ examples
import logging
import time
from contextlib import contextmanager
from threading import Thread
from pymq import Broker, TaskSet

log = logging.getLogger(__name__)


def simple(url, thread_worker):
    state = []

    # -- tasks.py --
    def func(arg):
        state.append(arg)

    broker = Broker(url)
    broker.publish(func)
    with thread_worker(broker):

        # -- task-invoking code --
        broker = Broker(url)
        default = broker.queue()
        res = default.func('arg')
        eq_(res, None)

        eventually((lambda:state), ['arg'])


def method_publishing(url, thread_worker):

    # -- tasks.py --
    class Database(object):
        """stateful storage"""
        value = None
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
    broker.publish(obj.update_value)
    with thread_worker(broker):

        # -- task-invoking code --
        broker = Broker(url)
        obj = broker.queue()
        res = obj.update_value(2)
        eq_(res, None)

        eventually((lambda:db.value), 2)


def taskset(url, thread_worker):
    """ Task sets

    - task is "pending" when it is in the queue.
    - task is "failed" when it is popped from the queue before being executed.
    - task changes from "failed" to "success" once executed successfully.
    - final task in task set cannot be executed until all other tasks in set
      have a result (either "failed" or "success").
    - "failed" tasks always have a result of None.
    - taskset algorithm:
        - Head tasks are enqueued to be executed in parallel.
        - Upon completion of each head task the results are checked to determine
          if all tasks have completed. If yes (only one task will get a YES),
          pop the results from the persistent store and enqueue the final task.
          Otherwise set a timeout on the results so they are not persisted
          forever if the taskset fails to complete for whatever reason.
    """

    # -- tasks.py --
    def func(arg):
        return arg
    def get_number(num):
        return num

    broker = Broker(url, 'not-the-default-queue')
    broker.publish(get_number)
    broker.publish(sum)
    with thread_worker(broker):

        # -- task-invoking code --
        broker = Broker(url)
        q = broker.queue('not-the-default-queue')
        tasks = TaskSet(result_timeout=5)
        for n in [1, 2, 3]:
            tasks.add(q.get_number, n)

        res = tasks(q.sum)

        assert res is not None
        eventually((lambda: res.value if res else None), 6)


def test():
    url = 'memory://'
    broker = Broker(url) # keep a reference so we always get the same one
    @contextmanager
    def thread_worker(broker):
        broker.start_worker()
        yield

    simple(url, thread_worker)
    method_publishing(url, thread_worker)
    taskset(url, thread_worker)

def test_redis():
    url = 'redis://localhost:6379/0'
    @contextmanager
    def thread_worker(broker):
        def run():
            try:
                broker.start_worker()
            except:
                log.error('worker crashed', exc_info=True)
        t = Thread(target=run)
        t.daemon = True # TODO remove
        t.start()
        try:
            yield
        finally:
            if t.is_alive():
                broker.stop()
                t.join()

    simple(url, thread_worker)
    method_publishing(url, thread_worker)
    taskset(url, thread_worker)


def eventually(condition, value, timeout=1):
    end = time.time() + timeout
    while time.time() < end:
        result = condition()
        if result:
            eq_(result, value)
            return
    raise AssertionError('eventuality failed to occur: %r' % (value,))

def eq_(value, other):
    assert value == other, '%r != %r' % (value, other)
