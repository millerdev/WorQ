# WorQ - Python task queue
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
from collections import defaultdict
from cPickle import (Pickler, PicklingError, Unpickler, UnpicklingError, loads,
    HIGHEST_PROTOCOL)
from cStringIO import StringIO
from uuid import uuid4
from weakref import ref as weakref

from worq.const import DEFAULT, HOUR, MINUTE, DAY, STATUS_VALUES, TASK_EXPIRED
from worq.task import (Queue, TaskSpace, FunctionTask, Deferred,
    TaskFailure, TaskExpired, DuplicateTask)

__all__ = ['Broker', 'AbstractTaskQueue']

log = logging.getLogger(__name__)

class Broker(object):
    """A Broker controlls all interaction with the queue backend"""

    def __init__(self, taskqueue):
        self._queue = taskqueue
        self.tasks = {}
        self.name = taskqueue.name

    @property
    def url(self):
        return self._queue.url

    def expose(self, obj, replace=False):
        """Expose a TaskSpace or task callable.

        :param obj: A TaskSpace or task callable.
        :param replace: Replace existing task if True. Otherwise (by default),
            raise ValueError if this would replace an existing task.
        """
        if isinstance(obj, TaskSpace):
            space = obj
        else:
            space = TaskSpace()
            space.task(obj)
        for name, func in space.tasks.iteritems():
            if name in self.tasks and not replace:
                raise ValueError('task %r conflicts with existing task' % name)
            self.tasks[name] = func

    def discard_pending_tasks(self):
        """Discard pending tasks from queue"""
        self._queue.discard_pending()

    def queue(self, target='', **options):
        """Get a Queue from the broker"""
        return Queue(self, target, **options)

    def __len__(self):
        """Return the approximate number of unprocessed tasks in the queue"""
        return self._queue.size()

    def enqueue(self, task):
        message, args = self.serialize(task, deferred=True)
        result = Deferred(self, task)
        queue = self._queue
        if args:
            if not queue.defer_task(result, message, args):
                raise DuplicateTask(task.name, self.name, task.id,
                    'cannot enqueue task with duplicate id')
            log.debug('defer %s [%s:%s]', task.name, self.name, task.id)
            for arg_id, arg in args.items():
                if arg.has_value():
                    msg = self.serialize(arg.value)
                else:
                    ok, msg = queue.reserve_argument(arg_id, task.id)
                    if not ok:
                        raise TaskFailure(task.name, self.name, task.id,
                            'task [%s:%s] result is not available'
                            % (self.name, arg_id))
                if msg is not None:
                    allset = queue.set_argument(task.id, arg_id, msg)
                    if allset:
                        log.debug('undefer %s [%s:%s]',
                            task.name, self.name, task.id)
                        queue.undefer_task(task.id)
        else:
            if not queue.enqueue_task(result, message):
                raise DuplicateTask(task.name, self.name, task.id,
                    'cannot enqueue task with duplicate id')
            log.debug('enqueue %s [%s:%s]', task.name, self.name, task.id)
        return None if task.ignore_result else result

    def status(self, result):
        """Get the status of a deferred result"""
        message = self._queue.get_status(result.id)
        if message is None:
            return message
        if message in STATUS_VALUES:
            return message
        return self.deserialize(message)

    def next_task(self, timeout=None):
        """Get the next task from the queue.

        :param timeout: See ``AbstractTaskQueue.get``.
        :returns: A task object. ``None`` on timeout expiration or if the task
            could not be deserialized.
        """
        message = self._queue.get(timeout=timeout)
        if message is None:
            return message
        task_id, message = message
        try:
            task = self.deserialize(message, task_id)
        except Exception:
            log.error('cannot deserialize task [%s:%s]',
                self.name, task_id, exc_info=True)
            return None
        #log.debug('next %s [%s:%s]', task.name, self.name, task_id)
        return task

    def invoke(self, task, **kw):
        """Invoke the given task (normally only called by a worker)"""
        return task.invoke(self, **kw)

    def heartbeat(self, task):
        """Extend task result timeout"""
        timeout = task.heartrate * 2 + 5
        self._queue.set_task_timeout(task.id, timeout)
        taskset_id = task.options.get('taskset_id')
        if taskset_id is not None:
            self._queue.set_task_timeout(taskset_id, timeout)

    def serialize(self, obj, deferred=False):
        """Serialize an object

        :param obj: The object to serialize.
        :param deferred: When this is true Deferred objects are
            serialized and their values are loaded on deserialization.
            When this is false Deferred objects are not serializable.
        """
        if deferred:
            args = {}
            def persistent_id(obj):
                if isinstance(obj, Deferred):
                    args[obj.id] = obj
                    return obj.id
                return None
        else:
            args = None
            def persistent_id(obj):
                if isinstance(obj, Deferred):
                    raise PicklingError('%s cannot be serialized' % obj)
                return None
        data = StringIO()
        pickle = Pickler(data, HIGHEST_PROTOCOL)
        pickle.persistent_id = persistent_id
        pickle.dump(obj)
        msg = data.getvalue()
        return (msg, args) if deferred else msg

    def deserialize(self, message, task_id=None):
        """Deserialize an object

        :param message: A serialized object (string).
        :param deferred: When true load deferreds. When false
            raise an error if the message contains deferreds.
        """
        fail = []
        if task_id is None:
            persistent_load = []
        else:
            args = self._queue.get_arguments(task_id)
            args = {k: loads(v) for k, v in args.items()}
            def persistent_load(arg_id):
                value = args[arg_id]
                if isinstance(value, TaskFailure):
                    fail.append(value)
                return value
        data = StringIO(message)
        pickle = Unpickler(data)
        pickle.persistent_load = persistent_load
        obj = pickle.load()
        if task_id is None and persistent_load:
            raise UnpicklingError('message contained references to '
                'external objects: %s', persistent_load)
        if fail and not obj.on_error_pass:
            # TODO detect errors earlier, fail earlier, cancel enqueued tasks
            self.set_result(obj, fail[0])
            obj = None
        return obj

    def set_result(self, task, result):
        """Persist result object.

        :param task: Task object for which to set the result.
        :param result: Result object.
        """
        if task.ignore_result:
            return
        timeout = task.result_timeout
        message = self.serialize(result)
        reserve_id = self._queue.set_result(task.id, message, timeout)
        if reserve_id is not None:
            allset = self._queue.set_argument(reserve_id, task.id, message)
            if allset:
                log.debug('undefer [%s:%s]', self.name, reserve_id)
                self._queue.undefer_task(reserve_id)

    def pop_result(self, task, timeout=0):
        """Pop and deserialize a task's result object

        :param task: An object with ``id`` and ``name`` attributes
            representing the task.
        :param timeout: Length of time to wait for the result. The default
            behavior is to return immediately (no wait). Wait indefinitely
            if ``None``.
        :returns: The deserialized result object.
        :raises: KeyError if the result was not available.
        :raises: TaskExpired if the task expired before a result was returned.
            A task normally only expires if the pool loses its ability
            to communicate with the worker performing the task.
        """
        if timeout < 0:
            raise ValueError('negative timeout not supported')
        message = self._queue.pop_result(task.id, timeout)
        if message is None:
            raise KeyError(task.id)
        if message is TASK_EXPIRED:
            result = message
        else:
            result = self.deserialize(message)
        if result is TASK_EXPIRED:
            raise TaskExpired(task.name, self.name, task.id,
                'task expired before a result was returned')
        return result

    def task_failed(self, task):
        """Signal that the given task has failed."""
        self._queue.discard_result(task.id, self.serialize(TASK_EXPIRED))


class AbstractTaskQueue(object):
    """Message queue abstract base class

    Task/result lifecycle

        1. Atomically store non-expiring result placeholder and enqueue task.
        2. Atomically pop task from queue and set timeout on result placeholder.
        3. Task heartbeats extend result expiration as needed.
        4. Task finishes and result value is saved.

    All methods must be thread-safe.

    :param url: URL used to identify the queue.
    :param name: Queue name.
    """

    def __init__(self, url, name=DEFAULT):
        self.url = url
        self.name = name

    def enqueue_task(self, result, message):
        """Enqueue task

        :param result: A ``Deferred`` result for the task.
        :param message: Serialized task message.
        :returns: True if the task was enqueued, otherwise False
            (duplicate task id).
        """
        raise NotImplementedError('abstract method')

    def defer_task(self, result, message, args):
        """Defer a task until its arguments become available

        :param result: A ``Deferred`` result for the task.
        :param message: The serialized task message.
        :param args: A list of task identifiers whose results will be
            included in the arguments to the task.
        """
        raise NotImplementedError('abstract method')

    def undefer_task(self, task_id):
        """Enqueue a deferred task

        All deferred arguments must be available immediately.
        """
        raise NotImplementedError('abstract method')

    def get(self, timeout=None):
        """Atomically get a serialized task message from the queue

        Task processing has started when this method returns, which
        means that the task heartbeat must be maintained if there could
        be someone waiting on the result. The result status is set to
        ``worq.const.PROCESSING`` if a result is being maintained for
        the task.

        :param timeout: Number of seconds to wait before returning ``None`` if
            no task is available in the queue. Wait forever if timeout is
            ``None``.
        :returns: A two-tuple (<task_id>, <serialized task message>) or
            ``None`` if timeout was reached before a task arrived.
        """
        raise NotImplementedError('abstract method')

    def size(self):
        """Return the approximate number of tasks in the queue"""
        raise NotImplementedError('abstract method')

    def discard_pending(self):
        """Discard pending tasks from queue"""
        raise NotImplementedError('abstract method')

    def reserve_argument(self, argument_id, deferred_id):
        """Reserve the result of a task as an argument of a deferred task

        :param argument_id: Identifier of a task whose result will
            be reserved for another task.
        :param deferred_id: Identifier of a deferred task who will get
            the reserved result as an argument.
        :returns: A two-tuple: (<bool>, <str>). The first item is a flag
            denoting if the argument was reserved, and the second is
            the serialized result if it was available else ``None``.
        """
        raise NotImplementedError('abstract method')

    def set_argument(self, task_id, argument_id, message):
        """Set deferred argument for task

        :param task_id: The identifier of the task to which the argument will
            be passed.
        :param argument_id: The argument identifier.
        :param message: The serialized argument value.
        :returns: True if all arguments have been set for the task.
        """
        raise NotImplementedError('abstract method')

    def get_arguments(self, task_id):
        """Get a dict of deferred arguments

        :param task_id: The identifier of the task to which the arguments will
            be passed.
        :returns: A dict of serialized arguments keyed by argument id.
        """
        raise NotImplementedError('abstract method')

    def set_task_timeout(self, task_id, timeout):
        """Set a timeout on the task result

        Recursively set the timeout on the given task and all deferred
        tasks depending on this task's result.
        """
        raise NotImplementedError('abstract method')

    def get_status(self, task_id):
        """Get the status of a task

        :param task_id: Unique task identifier string.
        :returns: A serialized task status object or ``None``.
        """
        raise NotImplementedError('abstract method')

    def set_result(self, task_id, message, timeout):
        """Persist serialized result message.

        This also sets the result status to ``worq.const.COMPLETED``.

        :param task_id: Unique task identifier string.
        :param message: Serialized result message.
        :param timeout: Number of seconds to persist the result before
            discarding it.
        :returns: A deferred task identifier if the result has been reserved.
            Otherwise ``None``.
        """
        raise NotImplementedError('abstract method')

    def pop_result(self, task_id, timeout):
        """Pop serialized result message from persistent storage.

        :param task_id: Unique task identifier string.
        :param timeout: Number of seconds to wait for the result. Wait
            indefinitely if ``None``. Return immediately if timeout is zero (0).
        :returns: One of the following:

            * The result message.
            * ``worq.const.RESERVED`` if another task depends on the result.
            * ``worq.const.TASK_EXPIRED`` if the task expired before a
              result was available.
            * ``None`` on timeout.

        """
        raise NotImplementedError('abstract method')

    def discard_result(self, task_id, task_expired_token):
        """Discard the result for the given task.

        A call to ``pop_result`` after this is invoked should return a
        task expired response.

        :param task_id: The task identifier.
        :param task_expired_token: A message that can be sent to blocking
            actors to signify that the task has expired.
        """
        raise NotImplementedError('abstract method')
