import logging
from collections import defaultdict
from pickle import dumps, loads
from uuid import uuid4
from weakref import ref as weakref

DEFAULT = 'default'
log = logging.getLogger(__name__)

class BaseBroker(object):

    default_result_timeout = 180

    def __init__(self, url, *queues):
        self.url = url
        if ':' in url.netloc:
            host, port = url.netloc.rsplit(':', 1)
            self.host = host
            self.port = int(port)
        else:
            self.host = url.netloc
            self.port = None
        self.path = url.path
        self.queues = list(queues) if queues else [DEFAULT]
        self.tasks = {stop_task.name: stop_task}

    def publish(self, callable, name=None):
        """Publish a task callable on all queues.

        :param callable: A function, method, or other callable.
        :param name: The task name. The callable's `__name__` will be used if
            this parameter is not given.
        """
        if name is None:
            name = callable.__name__
        if name in self.tasks:
            raise ValueError(
                'cannot publish two tasks with the same name: %s' % name)
        log.debug('published: %s on %s', name, self.queues)
        self.tasks[name] = callable

    def start_worker(self):
        """Start a worker

        This is normally a blocking call.
        """
        try:
            self.subscribe(self.queues)
        except StopBroker:
            log.info('broker stopped')

    def stop(self):
        """Stop a random worker to the queue.

        WARNING this is only meant for testing purposes. It will likely not do
        what you expect in an environment with more than one worker.
        """
        self.enqueue(self.queues[0], 'stop', stop_task.name, (), {}, {})

    def queue(self, queue=DEFAULT):
        return Queue(self, queue)

    def enqueue(self, queue, task_id, task_name, args, kw, options):
        message = dumps((task_id, task_name, args, kw, options))
        if options.get('result_timeout') is not None:
            result = self.deferred_result(task_id)
        else:
            result = None
        self.push_task(queue, message)
        return result

    def invoke(self, queue, message):
        try:
            task_id, task_name, args, kw, options = loads(message)
        except Exception:
            log.error('cannot load task message: %s', message, exc_info=True)
            return
        log.debug('task %s.%s [%s]', queue, task_name, task_id)
        try:
            error = True
            result = None
            try:
                task = self.tasks[task_name]
            except KeyError:
                log.error('no such task %r in queue %r', task_name, queue)
                return
            try:
                result = task(*args, **kw)
                error = False
            except Exception, err:
                log.error('%r task failed:', task_name, exc_info=True)
        finally:
            if 'taskset' in options:
                self.process_taskset(queue, options['taskset'], result)
            else:
                timeout = options.get('result_timeout')
                if timeout is not None:
                    log.debug('set %s [%s] -> %r', task_name, task_id, result)
                    message = dumps((error, result))
                    self.set_result_message(task_id, message, timeout)

    def process_taskset(self, queue, taskset, result):
        taskset_id, task_name, args, kw, options, num = taskset
        timeout = options.get('result_timeout', self.default_result_timeout)
        results = self.update_results(taskset_id, num, dumps(result), timeout)
        if results is not None:
            args = ([loads(r) for r in results],) + args
            self.enqueue(queue, taskset_id, task_name, args, kw, options)

    def deferred_result(self, task_id):
        return DeferredResult(self, task_id)

    def pop_result(self, task_id):
        message = self.pop_result_message(task_id)
        if message is None:
            return None
        return loads(message)

    def subscribe(self, queues):
        """Begin listening for task messages on the given queues.

        Must be implemented by each broker implementation. Not normally called
        by user code. This is often a blocking call.
        """
        raise NotImplementedError('abstract method')

    def push_task(self, queue, message):
        """Push a task message onto a named task queue.

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param queue: Queue name.
        :param message: Serialized task message.
        """
        raise NotImplementedError('abstract method')

    def set_result_message(self, task_id, message, timeout):
        """Persist serialized result message.

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :param message: Serialized task message.
        :param timeout: Number of seconds to persist the result before
            discarding it.
        """
        raise NotImplementedError('abstract method')

    def pop_result_message(self, task_id):
        """Pop serialized result message from persistent storage.

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :returns: The result message; None if not found.
        """
        raise NotImplementedError('abstract method')

    def update_results(self, taskset_id, num_tasks, message, timeout):
        """Update the result set, returning all results if complete

        Must be implemented by each broker implementation. Not normally called
        by user code. This operation is atomic, meaning that only one caller
        will ever be returned a value other than None for a given `taskset_id`.

        :param taskset_id: (string) The taskset unique identifier.
        :param num_tasks: (int) Number of tasks in the set.
        :param message: (string) A serialized result object to add to the
            list of results.
        :param timeout: (int) Discard results after this number of seconds.
        :returns: None if the number of updates has not reached num_tasks.
            Otherwise return an unordered list of serialized result messages.
        """
        raise NotImplementedError('abstract method')


class Queue(object):
    """Queue object for invoking remote tasks"""

    def __init__(self, broker, name):
        self.__broker = broker
        self.__queue = name

    def __getattr__(self, name):
        return Task(self.__broker, self.__queue, name)

    def __repr__(self):
        return '<Queue name=%s>' % self.__queue


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

    def __repr__(self):
        return '<Task %s.%s>' % (self.queue, self.name)


class DeferredResult(object):
    """Deferred result object

    Meaningful attributes:
    - value: The result value. This is set when evaluating the boolean value of
        the DeferredResult object after the task returns successfully.
    - completed: A boolean value denoting if the task has completed. Retrieving
        this value will NOT retrieve the value from the broker if it has not
        yet arrived.
    """

    def __init__(self, broker, task_id):
        self.broker = broker
        self.task_id = task_id
        self.completed = False

    def __nonzero__(self):
        """Return True if the result has arrived, otherwise False."""
        if not self.completed:
            result = self.broker.pop_result(self.task_id)
            if result is None:
                return False
            self.completed = True
            error, value = result
            if error:
                self.error = value
            else:
                self.value = value
        return True

    def __repr__(self):
        if self:
            if hasattr(self, 'error'):
                value = 'task failed'
            else:
                value = 'value=%r' % (self.value,)
        else:
            value = 'incomplete'
        return '<DeferredResult %s>' % (value,)


class TaskSet(object):
    """Execute a set of tasks in parallel, process results in a final task.

    :param result_timeout: Number of seconds to persist the final result. This
        timeout value is also used to retain intermediate task results. It
        should be longer than the longest-running task in the set. The default
        is None, which means the final result will be ignored; the default
        timeout for intermediate tasks is 3 minutes (180 seconds) in that case.

    Usage:
        >>> t = TaskSet(result_timeout=60)
        >>> for arg in [0, 1, 2, 3]:
        ...     t.add(q.plus_ten, arg)
        ...
        >>> t(q.sum)
        <DeferredResult value=46>

    TaskSet algorithm:
    - Head tasks (added with .add) are queued to be executed in parallel.
    - Upon completion of each head task the results are checked to determine
      if all head tasks have completed. If so, pop the results from the
      persistent store and enqueue the final task (passed to .__call__).
      Otherwise set a timeout on the results so they are not persisted forever
      if the taskset fails to complete for whatever reason.
    """

    def __init__(self, result_timeout=None):
        self.queue = None
        self.tasks = []
        self.options = {}
        if result_timeout is not None:
            self.options['result_timeout'] = result_timeout

    def add(*self_task_args, **kw):
        """Add a task to the set

        :params task: A task object.
        :params *args: Positional arguments to use when invoking the task.
        :params **kw: Keyword arguments to use when invoking the task.
        """
        if len(self_task_args) < 2:
            raise ValueError('expected at least two positional '
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
        """Invoke the taskset

        :params task: The final task object, which will be invoked with a list
            of results from all other tasks in the set as its first argument.
        :params *args: Extra positional arguments to use when invoking the task.
        :params **kw: Keyword arguments to use when invoking the task.
        :returns: None if the TaskSet was created with `result_timeout=None`.
            Otherwise, a DeferredResult object.
        """
        TaskSet.add(*self_task_args, **kw)
        self = self_task_args[0]
        task, args, kw = self.tasks.pop()
        broker, queue = self.queue
        num = len(self.tasks)
        taskset_id = uuid4().hex
        if self.options.get('result_timeout') is not None:
            result = broker.deferred_result(taskset_id)
        else:
            result = None
        options = {'taskset':
            (taskset_id, task.name, args, kw, self.options, num)}
        for t, a, k in self.tasks:
            t.with_options(**options)(*a, **k)
        return result


class StopBroker(BaseException): pass

def stop_task():
    raise StopBroker()
stop_task.name = '<stop_task>'
