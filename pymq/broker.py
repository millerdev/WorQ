import logging
from collections import defaultdict
from pickle import dumps, loads
from uuid import uuid4
from weakref import ref as weakref

DEFAULT = 'default'
log = logging.getLogger(__name__)

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
        self.workers = {}

    def worker(self, *queues):
        # TODO eliminate this method (add broker.publish)
        if not queues:
            queues = [DEFAULT]
        worker = Worker(self, queues)
        for queue in queues:
            self.workers[queue] = worker
        return worker

    def queue(self, queue=DEFAULT):
        return Queue(self, queue)

    def enqueue(self, queue, task_id, task_name, args, kw, options):
        blob = dumps((task_id, task_name, args, kw, options))
        result = self.deferred_result(task_id)
        self.push_task(queue, blob)
        return result

    def invoke(self, queue, blob):
        try:
            task_id, task_name, args, kw, options = loads(blob)
        except Exception:
            log.error('cannot load task blob: %s', blob, exc_info=True)
            return
        if 'taskset' in options:
            taskset_id = options['taskset'][0]
        try:
            worker = self.workers[queue]
        except KeyError:
            log.error('cannot find worker for queue: %s', queue, exc_info=True)
            return
        try:
            task = worker.tasks[task_name]
        except KeyError:
            log.error('no such task %r on queue %r',
                task_name, queue, exc_info=True)
            return
        error = False
        try:
            result = task(*args, **kw)
        except Exception, err:
            log.error('task failed: %s -> %s', task_name, err, exc_info=True)
            error = True
            result = str(err)
        self.set_result_blob(task_id, dumps((error, result)))

    def deferred_result(self, task_id):
        return Result(self, task_id)

    def pop_result(self, task_id):
        blob = self.pop_result_blob(task_id)
        if blob is None:
            return None
        return loads(blob)

    def subscribe(self, worker, queues):
        raise NotImplementedError('abstract method')

    def push_task(self, queue, blob):
        raise NotImplementedError('abstract method')

    def set_result_blob(self, task_id, blob):
        raise NotImplementedError('abstract method')

    def pop_result_blob(self, task_id):
        raise NotImplementedError('abstract method')


class Worker(object):

    def __init__(self, broker, queues):
        self._broker = weakref(broker)
        self.queues = queues
        self.tasks = {}

    @property
    def broker(self):
        return self._broker()

    def publish(self, callable, name=None):
        if name is None:
            name = callable.__name__
        if name in self.tasks:
            raise ValueError(
                'cannot publish two tasks with the same name: %s' % name)
        self.tasks[name] = callable

    def start(self):
        self.broker.subscribe(self, self.queues)


class Queue(object):

    def __init__(self, broker, queue):
        self.__broker = broker
        self.__queue = queue

    def __getattr__(self, name):
        return Task(self.__broker, self.__queue, name)


class Task(object):

    def __init__(self, broker, queue, name, options={}):
        self.broker = broker
        self.queue = queue
        self.name = name
        self.opts = options

    def __call__(self, *args, **kw):
        id = uuid4().hex
        return self.broker.enqueue(
            self.queue, id, self.name, args, kw, self.opts)

    def with_options(self, **options):
        return Task(self.broker, self.queue, self.name, options)


class Result(object):

    def __init__(self, broker, task_id):
        self.broker = broker
        self.task_id = task_id
        self._completed = False

    @property
    def completed(self):
        if not self._completed:
            result = self.broker.pop_result(self.task_id)
            if result is None:
                return False
            self._completed = True
            error, value = result
            if error:
                self.error = value
            else:
                self.value = value
        return True

    def __repr__(self):
        if self.completed:
            if self.error:
                value = 'error: %s' % (self.value,)
            else:
                value = 'value=%r' % (self.value,)
        else:
            value = 'incomplete'
        return '<Result id=%s %s>' % (self.task_id, value)


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
            if self.queue is None:
                self.queue = (task.broker, task.queue)
            elif self.queue != (task.broker, task.queue):
                raise ValueError('cannot combine tasks from discrete queues')
        else:
            task = resultset
            args = ()
            if kw:
                raise ValueError('unexected keyword arguments: %r' % (kw,))
        broker, queue = self.queue
        num = len(self.tasks)
        options = {'taskset': (uuid4().hex, task.name, args, kw, num)}
        for t, a, k in self.tasks:
            t.with_options(**options)(*a, **k)
        return broker.deferred_result(options['taskset'][0])

def resultset(results):
    return results
resultset.name = '<resultset>'
