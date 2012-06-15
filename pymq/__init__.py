# PyMQ implementation
import redis
from urlparse import urlparse
DEFAULT = 'default'


def Broker(url):
    data = urlparse(url)
    if data.scheme == 'null':
        return NULL_BROKER
    elif data.scheme == 'redis':
        return RedisBroker(data)
    raise ValueError('invalid broker URL: %s' % url)


class BaseBroker(object):

    def __init__(self, url):
        self.url = url
        if ':' in url.netloc:
            host, port = url.netloc.rsplit(':', 1)
            self.host = host
            self.port = int(port)
        else:
            self.host = url.netloc
            self.port = None
        self.path = url.path

    def worker(self, *queues):
        if not queues:
            queues = [DEFAULT]
        return Worker(self, queues)

    def queue(self, queue=DEFAULT):
        return Queue(self, queue)


class RedisBroker(BaseBroker):

    def __init__(self, *args, **kw):
        super(RedisBroker, self).__init__(*args, **kw)
        db = int(self.path.lstrip('/'))
        self.connection = redis.Redis(self.host, self.port, db=db)


class NullBroker(BaseBroker):

    def __init__(self, *args, **kw):
        super(NullBroker, self).__init__(*args, **kw)
        self.workers = {}

    def invoke(self, queue, task, args, kwargs):
        result = self.workers[queue].invoke(task, args, kwargs)
        return Result(result, completed=True)

    def invoke_multiple(self, queue, tasks, task, args, kwargs):
        results = (t(*a, **k) for t, a, k in tasks)
        result = task({r.value for r in results}, *args, **kwargs)
        if task is not resultset:
            result = result.value
        return Result(result, completed=True)

    def subscribe(self, worker, queue):
        self.workers[queue] = worker

NULL_BROKER = NullBroker(urlparse('null://'))


class Worker(object):

    def __init__(self, broker, queues):
        self.broker = broker
        self.queues = queues
        self.tasks = {}

    def publish(self, callable, name=None):
        if name is None:
            name = callable.__name__
        if name in self.tasks:
            raise ValueError(
                'cannot publish two tasks with the same name: %s' % name)
        self.tasks[name] = callable

    def invoke(self, task, args, kwargs):
        task = self.tasks[task]
        return task(*args, **kwargs)

    def start(self):
        for queue in self.queues:
            self.broker.subscribe(self, queue)


class Queue(object):

    def __init__(self, broker, queue):
        self.__broker = broker
        self.__queue = queue

    def __getattr__(self, name):
        return Task(self.__broker, self.__queue, name)


class Task(object):

    def __init__(self, broker, queue, name):
        self.broker = broker
        self.queue = queue
        self.name = name

    def __call__(self, *args, **kw):
        return self.broker.invoke(self.queue, self.name, args, kw)


class Result(object):

    def __init__(self, value=None, error=None, completed=False):
        self.value = value
        self.error = error
        self.completed = completed

    def __repr__(self):
        if self.completed:
            if self.error:
                raise NotImplementedError
            value = 'value=%r' % (self.value,)
        else:
            value = 'incomplete'
        return '<Result %s>' % value


class TaskSet(object):

    def __init__(self):
        self.queue = None
        self.tasks = []

    def add(*self_task_args, **kw):
        if len(self_task_args) < 2:
            raise ValueError('TaskSet.add expected at least two positional '
                'arguments, got %s' % len(self_task_args))
        self = self_task_args[0]
        task = self_task_args[1]
        args = self_task_args[2:]
        if self.queue is None:
            self.queue = (task.broker, task.queue)
        elif self.queue != (task.broker, task.queue):
            raise ValueError('cannot combine tasks from discrete queues')
        self.tasks.append((task, args, kw))

    def __call__(*self_task_args, **kw):
        if len(self_task_args) < 1:
            raise ValueError('TaskSet.__call__ expected at least one '
                'positional argument, got %s' % len(self_task_args))
        self = self_task_args[0]
        if len(self_task_args) > 1:
            task = self_task_args[1]
            args = self_task_args[2:]
        else:
            task = resultset
            args = ()
            if kw:
                raise ValueError('unexected keyword arguments: %r' % (kw,))
        broker, queue = self.queue
        return broker.invoke_multiple(queue, self.tasks, task, args, kw)

def resultset(results):
    return results
