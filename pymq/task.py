import time
from uuid import uuid4

from pymq.const import DEFAULT


class Queue(object):
    """Queue for invoking remote tasks

    New Queue instances are generated through attribute access. For example::

        >>> q = Queue(broker)
        >>> q.foo
        <Queue foo [default]>
        >>> q.foo.bar
        <Queue foo.bar [default]>

    A Queue instance can be called like a function, which invokes a remote
    task identified by the target of the Queue instance. Example::

        >>> q = Queue(broker)
        >>> q.foo.func(1, key=None) # enqueue task to be invoked by worker

    NOTE two queue objects are considered equal if they refer to the same
    queue on the same broker (their targets may be different).
    """

    def __init__(self, broker, queue=DEFAULT, target=''):
        self.__broker = broker
        self.__queue = queue
        self.__target = target

    def __getattr__(self, target):
        if self.__target:
            target = '%s.%s' % (self.__target, target)
        return Queue(self.__broker, self.__queue, target)

    def __call__(self, *args, **kw):
        """Invoke the task identified by this Queue"""
        return Task(self)(*args, **kw)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return (self.__broker, self.__queue) == (other.__broker, other.__queue)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '<Queue %s [%s]>' % (self.__target, self.__queue)

    def __str__(self):
        return self.__target


class Task(object):
    """Remote task handle

    This class can be used to construct a task with custom options. A task
    is invoked by calling the task object.

    :param queue: The Queue object on which this task should be executed.
    :param **options: Custom task options.
    """

    def __init__(self, queue, **options):
        self.queue = queue
        self.name = queue._Queue__target
        self.options = options

    @property
    def broker(self):
        return self.queue._Queue__broker

    def __call__(self, *args, **kw):
        id = uuid4().hex
        return self.queue._Queue__broker.enqueue(
            self.queue._Queue__queue, id, self.name, args, kw, self.options)

    def with_options(self, options):
        """Clone this task with a new set of options"""
        return Task(self.queue, **options)

    def __repr__(self):
        return '<Task %s [%s]>' % (self.name, self.queue._Queue__queue)


class TaskFailure(Exception):

    @property
    def task_name(self):
        return self.args[0]

    @property
    def queue(self):
        return self.args[1]

    @property
    def task_id(self):
        return self.args[2]

    @property
    def error(self):
        return self.args[3]

    def __str__(self):
        return '%s [%s:%s] %s' % self.args

    def __repr__(self):
        return '<TaskFailure %s>' % self

    def __eq__(self, other):
        return isinstance(other, TaskFailure) and repr(self) == repr(other)

    def __ne__(self, other):
        return not self.__eq__(other)


class DeferredResult(object):
    """Deferred result object

    Meaningful attributes:
    - value: The result value. This is set when evaluating the boolean value of
      the DeferredResult object after the task completes. The value of this
      attribute will be an instance of TaskFailure if the task could not be
      invoked or raised an error.
    """

    def __init__(self, store, task_id):
        self.store = store
        self.id = task_id
        self._completed = False

    @property
    def value(self):
        """Get the value returned by the task (if completed)

        :returns: The value returned by the task if it completed successfully.
        :raises: AttributeError if the task has not yet completed. TaskFailure
            if the task could not be invoked or raised an error.
        """
        if isinstance(self._value, TaskFailure):
            raise self._value
        return self._value

    def wait(self, timeout=None, poll_interval=1):
        """Wait for the task result.

        This method polls the result store. Use carefully. A task calling this
        method (waiting on the result of a task being executed by the
        same group of workers as the waiting task) may result in dead lock.

        :param timeout: Number of seconds to wait. Wait forever by default.
        :param poll_interval: Number of seconds to sleep between polling the
            result store. Default 1 second.
        :returns: True if the task completed, otherwise False.
        """
        if timeout is None:
            while not self:
                time.sleep(poll_interval)
        else:
            end = time.time() + timeout
            while not self and end > time.time():
                time.sleep(poll_interval)
        return self._completed

    def __nonzero__(self):
        """Return True if the result has arrived, otherwise False."""
        if not self._completed:
            value = self.store.pop(self.id)
            if value is None:
                return False
            assert len(value) == 1, value
            self._value = value[0]
            self._completed = True
        return True

    def __repr__(self):
        if self:
            if isinstance(self._value, TaskFailure):
                status = 'failed'
            else:
                status = 'success'
        else:
            status = 'incomplete'
        return '<DeferredResult %s %s>' % (self.id, status)


class TaskSet(object):
    """Execute a set of tasks in parallel, process results in a final task.

    :param result_timeout: Number of seconds to persist the final result. This
        timeout value is also used to retain intermediate task results. It
        should be longer than the longest-running task in the set. The default
        is None, which means the final result will be ignored; the default
        timeout for intermediate tasks is 24 hours in that case.

    Usage:
        >>> t = TaskSet(result_timeout=60)
        >>> for arg in [0, 1, 2, 3]:
        ...     t.add(q.plus_ten, arg)
        ...
        >>> t(q.sum)
        <DeferredResult value=46>

    TaskSet algorithm:
    - Head tasks (added with .add) are enqueued to be executed in parallel
      when the TaskSet is called with the final task.
    - Upon completion of each head task the results are checked to determine
      if all head tasks have completed. If so, pop the results from the
      persistent store and enqueue the final task (passed to .__call__).
      Otherwise set a timeout on the results so they are not persisted forever
      if the taskset fails to complete for whatever reason.
    """

    def __init__(self, **options):
        self.queue = None
        self.tasks = []
        self.options = options

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
        if isinstance(task, Queue):
            task = Task(task)
        args = self_task_args[2:]
        if self.queue is None:
            self.queue = task.queue
        elif self.queue != task.queue:
            raise ValueError('cannot combine tasks from discrete queues: '
                '%s != %s' % (self.queue, task.queue))
        self.tasks.append((task, args, kw))

    def with_options(self, options):
        ts = TaskSet(**options)
        ts.queue = self.queue
        ts.tasks = list(self.tasks)
        return ts

    def __call__(*self_task_args, **kw):
        """Invoke the taskset

        :params task: The final task object, which will be invoked with a list
            of results from all other tasks in the set as its first argument.
        :params *args: Extra positional arguments to use when invoking the task.
        :params **kw: Keyword arguments to use when invoking the task.
        :returns: DeferredResult if the TaskSet was created with a
            `result_timeout`. Otherwise, None.
        """
        TaskSet.add(*self_task_args, **kw)
        self = self_task_args[0]
        task, args, kw = self.tasks.pop()
        num = len(self.tasks)
        taskset_id = uuid4().hex
        if self.options.get('result_timeout') is not None:
            result = task.broker.deferred_result(taskset_id)
        else:
            result = None
        options = {'taskset':
            (taskset_id, task.name, args, kw, self.options, num)}
        for t, a, k in self.tasks:
            t.with_options(options)(*a, **k)
        return result


class TaskSpace(object):
    """Task namespace container"""

    def __init__(self, name=''):
        self.name = name
        self.tasks = {}

    def task(self, callable, name=None):
        """Add a task to the namespace

        This can be used as a decorator::
            ts = TaskSpace(__name__)

            @ts.task
            def frob(value):
                db.update(value)

        :param callable: A callable object, usually a function or method.
        :param name: Task name. Defaults to `callable.__name__`.
        """
        if name is None:
            name = callable.__name__
        if self.name:
            name = '%s.%s' % (self.name, name)
        if name in self.tasks:
            raise ValueError('task %r conflicts with existing task' % name)
        self.tasks[name] = callable
        return callable
