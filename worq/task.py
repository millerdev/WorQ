# WorQ - asynchronous Python task queue.
#
# Copyright (c) 2012 Daniel Miller
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
import time
from uuid import uuid4

from worq.const import DAY, DEFAULT, PROCESSING

log = logging.getLogger(__name__)


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

        # Enqueue task 'func' in namespace 'foo' to be invoked
        # by a worker listening on the 'default' queue.
        >>> q = Queue(broker)
        >>> q.foo.func(1, key=None)

    The arrangement of Queue tasks in task spaces is similar to Python's system
    of defining functions in modules and packages.

    NOTE two queue objects are considered equal if they refer to the same
    broker (their targets may be different).
    """

    def __init__(self, broker, target=''):
        self.__broker = broker
        self.__target = target

    def __getattr__(self, target):
        if self.__target:
            target = '%s.%s' % (self.__target, target)
        return Queue(self.__broker, target)

    def __call__(self, *args, **kw):
        """Invoke the task identified by this Queue"""
        return Task(self)(*args, **kw)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self.__broker == other.__broker

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '<Queue %s [%s]>' % (self.__target, self.__broker.name)

    def __str__(self):
        return self.__target


class Task(object):
    """Remote task handle

    This class can be used to construct a task with custom options. A task
    is invoked by calling the task object.

    :param queue: The Queue object identifying the task to be executed.
    :param result_status: Default False. When True, send an extra keyword
        argument ('update_status') when invoking the task. The 'update_status'
        argument is a function that can be called to update the status of the
        task result.
    :param result_timeout: Timeout value for retaining the result in the result
        store. A default timeout of one day is used if 'result_timeout' is not
        specified and 'result_status' is True. If neither 'result_status' nor
        'result_timeout' are specified then the task result is ignored.
    """

    def __init__(self, queue, **options):
        self.queue = queue
        self.name = queue._Queue__target
        self.options = options

    @property
    def broker(self):
        return self.queue._Queue__broker

    def __call__(self, *args, **kw):
        task = FunctionTask(self.name, args, kw, self.options)
        return self.queue._Queue__broker.enqueue(task)

    def with_options(self, options):
        """Clone this task with a new set of options"""
        return Task(self.queue, **options)

    def __repr__(self):
        return '<Task %s [%s]>' % (self.name, self.queue._Queue__queue)


class FunctionTask(object):

    def __init__(self, name, args, kw, options):
        self.id = uuid4().hex
        self.name = name
        self.args = args
        self.kw = kw
        self.options = options

    @property
    def heartrate(self):
        return self.options.get('heartrate', 30)

    @property
    def result_timeout(self):
        return self.options.get('result_timeout', DAY)

    def invoke(self, broker):
        queue = broker.name
        options = self.options
        log.debug('invoke %s [%s:%s]', self.name, queue, self.id)
        result_status = options.get('result_status', False)
        if result_status:
            broker.set_status(self, PROCESSING)
            def update_status(value):
                broker.set_status(self, value)
            self.kw['update_status'] = update_status
        try:
            try:
                task = broker.tasks[self.name]
            except KeyError:
                result = TaskFailure(self.name, queue, self.id, 'no such task')
                log.error(result)
            else:
                result = task(*self.args, **self.kw)
        except Exception, err:
            log.error('task failed: %s [%s:%s]',
                self.name, queue, self.id, exc_info=True)
            result = TaskFailure(self.name, queue, self.id,
                '%s: %s' % (type(err).__name__, err))
        except BaseException, err:
            log.error('worker died in task: %s [%s:%s]',
                self.name, queue, self.id, exc_info=True)
            result = TaskFailure(self.name, queue, self.id,
                '%s: %s' % (type(err).__name__, err))
            raise
        finally:
            if 'taskset' in options:
                self.process_taskset(broker, options['taskset'], result)
            elif result_status or 'result_timeout' in options:
                broker.set_result(self, result)

    def process_taskset(self, broker, taskset, result):
        options = taskset.options
        if (options.get('on_error', TaskSet.FAIL) == TaskSet.FAIL
                and isinstance(result, TaskFailure)):
            # suboptimal: pending tasks in the set will continue to be executed
            # and their results will be persisted if they succeed.
            fail = TaskFailure(
                taskset.name, broker.name, taskset.id, 'subtask(s) failed')
            broker.set_result(taskset, fail)
        else:
            timeout = taskset.result_timeout
            results = broker.messages.update_taskset(
                taskset.id, options['size'], broker.serialize(result), timeout)
            if results is not None:
                loads = broker.deserialize
                taskset.args = ([loads(r) for r in results],) + taskset.args
                broker.enqueue(taskset)


class DeferredResult(object):
    """Deferred result object

    Not thread-safe.
    """

    def __init__(self, broker, task_id, task_name, heartrate):
        self.broker = broker
        self.id = task_id
        self.name = task_name
        self.heartrate = heartrate * 2
        self._status = None

    @property
    def value(self):
        """Get the value returned by the task (if completed)

        :returns: The value returned by the task if it completed successfully.
        :raises: AttributeError if the task has not yet completed. TaskFailure
            if the task failed for any reason.
        """
        self.wait(0)
        try:
            value = self._value
        except AttributeError:
            raise AttributeError('DeferredResult value not available')
        if isinstance(value, TaskFailure):
            raise value
        return value

    @property
    def status(self):
        """Get task status"""
        self._status = self.broker.status(self)
        return self._status

    def wait(self, timeout):
        """Wait for the task result.

        Use this method wisely. In general a task should never wait on the
        result of another task because it may cause deadlock.

        :param timeout: Number of seconds to wait. A value of None will wait
            indefinitely, but this is dangerous since the worker may go away
            without notice (due to loss of power, etc.) causing this method
            to deadlock.
        :returns: True if the result is available, otherwise False.
        """
        if not hasattr(self, '_value'):
            try:
                value = self.broker.pop_result(self, timeout=timeout)
            except KeyError:
                return False
            except TaskExpired, err:
                value = err
            self._value = value
        return hasattr(self, '_value')

    def __nonzero__(self):
        """Return True if the result has arrived, otherwise False."""
        return self.wait(0)

    def __repr__(self):
        if self:
            if isinstance(self._value, TaskFailure):
                status = 'failed'
            else:
                status = 'success'
        elif self._status is not None:
            status = self._status
        else:
            status = 'incomplete'
        args = (self.name, self.broker.name, self.id, status)
        return '<DeferredResult %s [%s:%s] %s>' % args


class TaskSet(object):
    """Execute a set of tasks in parallel, process results in a final task.

    :param result_timeout: Number of seconds to persist the final result. This
        timeout value is also used to retain intermediate task results. It
        should be longer than the longest-running task in the set. The default
        is None, which means the final result will be ignored; the default
        timeout for intermediate tasks is 24 hours in that case.
    :param on_error: What to do when one or more of the tasks in the set fail.
        * FAIL: (default) do not execute final task if any task fails.
        * PASS: Pass TaskFailure objects as the result of failed tasks.

    Usage:
        >>> t = TaskSet(result_timeout=60)
        >>> for arg in [0, 1, 2, 3]:
        ...     t.add(q.plus_ten, arg)
        ...
        >>> res = t(q.sum)
        >>> res.wait(60)
        True
        >>> res.value
        46

    TaskSet algorithm:
    - Head tasks (added with .add) are enqueued to be executed in parallel
      when the TaskSet is called with the final task.
    - Upon completion of each head task the results are checked to determine
      if all head tasks have completed. If so, pop the results from the
      persistent store and enqueue the final task (passed to .__call__).
      Otherwise set a timeout on the results so they are not persisted forever
      if the taskset fails to complete for whatever reason.
    """

    PASS = 'pass'
    FAIL = 'fail'

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
        opts = dict(self.options, size=len(self.tasks))
        taskset = FunctionTask(task.name, args, kw, opts)
        result = task.broker.init_taskset(taskset)
        result.__subsets = []
        options = {'taskset': taskset}
        for t, a, k in self.tasks:
            r = t.with_options(options)(*a, **k)
            # HACK should not set result attributes here. this needs to be formalized
            if isinstance(t, TaskSet) and result is not None:
                assert r is not None
                result.__subsets.append(r)
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


class TaskFailure(Exception):
    """Task failure exception class

    Initialize with the following positional arguments:
        1. Task name
        2. Queue name
        3. Task id
        4. Error text
    """

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
        return '<%s %s>' % (type(self).__name__, self)

    def __eq__(self, other):
        return isinstance(other, TaskFailure) and repr(self) == repr(other)

    def __ne__(self, other):
        return not self.__eq__(other)


class TaskExpired(TaskFailure): pass


class TaskStatus(object):
    """Task status value container/marker"""

    def __init__(self, value):
        self.value = value
