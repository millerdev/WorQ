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
from collections import defaultdict
from cPickle import dumps, loads
from weakref import ref as weakref

from worq.const import DEFAULT
from worq.task import (Queue, TaskSet, TaskSpace, DeferredResult,
    TaskStatus, TaskFailure)

log = logging.getLogger(__name__)

MINUTE = 60 # number of seconds in one minute
HOUR = MINUTE * 60 # number of seconds in one hour
DAY = HOUR * 24 # number of seconds in one day

class Broker(object):

    task_options = set([
        'result_status',
        'result_timeout',
        'taskset',
        'on_error',
    ])

    def __init__(self, message_queue):
        self.messages = message_queue
        self.tasks = {_stop_task.name: _stop_task}
        self.name = message_queue.name

    def expose(self, obj):
        """Expose a TaskSpace or task callable.

        :param obj: A TaskSpace or task callable.
        """
        if isinstance(obj, TaskSpace):
            space = obj
        else:
            space = TaskSpace()
            space.task(obj)
        for name, func in space.tasks.iteritems():
            if name in self.tasks:
                raise ValueError('task %r conflicts with existing task' % name)
            self.tasks[name] = func

    def start_worker(self):
        """Start a worker

        This is normally a blocking call.
        """
        try:
            for message in self.messages:
                self.invoke(message)
        except _StopWorker:
            log.info('worker stopped')

    def stop(self):
        """Stop a random worker.

        WARNING this is only meant for testing purposes. It will likely not do
        what you expect in an environment with more than one worker.
        """
        self.enqueue('stop', _stop_task.name, (), {}, {})

    def discard_pending_tasks(self):
        """Discard pending tasks from queue"""
        self.messages.discard_pending()

    def queue(self, target=''):
        return Queue(self, target)

    def enqueue(self, task_id, task_name, args, kw, options):
        queue = self.name # TODO remove this
        unknown_options = set(options) - self.task_options
        if unknown_options:
            raise ValueError('unrecognized task options: %s'
                % ', '.join(unknown_options))
        log.debug('enqueue %s [%s:%s]', task_name, queue, task_id)
        message = dumps((task_id, task_name, args, kw, options))
        result_status = options.get('result_status', False)
        if result_status or 'result_timeout' in options:
            result = self.messages.deferred_result(task_id)
            if result_status:
                timeout = options.get('result_timeout', DAY)
                status = TaskStatus('enqueued')
                self.messages.set_result(task_id, dumps(status), timeout)
        else:
            result = None
        self.messages.enqueue_task(task_id, message)
        return result

    def invoke(self, message):
        try:
            task_id, task_name, args, kw, options = loads(message)
        except Exception:
            log.error('cannot load task message: %s', message, exc_info=True)
            return
        queue = self.name
        log.debug('invoke %s [%s:%s]', task_name, queue, task_id)
        timeout = options.get('result_timeout', DAY)
        result_status = options.get('result_status', False)
        if result_status:
            status = TaskStatus('processing')
            self.messages.set_result(task_id, dumps(status), timeout)
            def update_status(value):
                status = TaskStatus(value)
                self.messages.set_result(task_id, dumps(status), timeout)
            kw['update_status'] = update_status
        try:
            try:
                task = self.tasks[task_name]
            except KeyError:
                result = TaskFailure(task_name, queue, task_id, 'no such task')
                log.error(result)
            else:
                result = task(*args, **kw)
        except _StopWorker:
            result = TaskFailure(task_name, queue, task_id, 'worker stopped')
            raise
        except Exception, err:
            log.error('task failed: %s [%s:%s]',
                task_name, queue, task_id, exc_info=True)
            result = TaskFailure(task_name, queue, task_id,
                '%s: %s' % (type(err).__name__, err))
        except BaseException, err:
            log.error('worker died in task: %s [%s:%s]',
                task_name, queue, task_id, exc_info=True)
            result = TaskFailure(task_name, queue, task_id,
                '%s: %s' % (type(err).__name__, err))
            raise
        finally:
            if 'taskset' in options:
                self.process_taskset(queue, options['taskset'], result)
            elif result_status or 'result_timeout' in options:
                message = dumps(result)
                self.messages.set_result(task_id, message, timeout)

    def process_taskset(self, queue, taskset, result):
        taskset_id, task_name, args, kw, options, num = taskset
        timeout = options.get('result_timeout', DAY)
        if (options.get('on_error', TaskSet.FAIL) == TaskSet.FAIL
                and isinstance(result, TaskFailure)):
            # suboptimal: pending tasks in the set will continue to be executed
            # and their results will be persisted if they succeed.
            message = dumps(TaskFailure(
                task_name, queue, taskset_id, 'subtask(s) failed'))
            self.messages.set_result(taskset_id, message, timeout)
        else:
            results = self.messages.update(
                taskset_id, num, dumps(result), timeout)
            if results is not None:
                args = ([loads(r) for r in results],) + args
                self.enqueue(taskset_id, task_name, args, kw, options)

    def deferred_result(self, task_id):
        return self.messages.deferred_result(task_id)


class AbstractMessageQueue(object):
    """Message queue abstract base class

    Task/result lifecycle
    1. Atomically store non-expiring result placeholder and enqueue task.
    2. Atomically pop task from queue and set timeout on result placeholder.
    3. Task heartbeats extend result expiration as needed.
    4. Task finishes and result value is saved.

    :param url: URL used to identify the queue.
    :param name: Queue name.
    """

    def __init__(self, url, name=DEFAULT):
        self.url = url
        self.name = name

    def enqueue_task(self, task_id, message):
        """Enqueue task

        :param task_id: Task identifier.
        :param message: Serialized task message.
        """
        raise NotImplementedError('abstract method')

    def get(self, timeout=None):
        """Atomically get a serialized task message from the queue

        Task processing has started when this method returns, which
        means that the task heartbeat must be maintained if there
        could be someone waiting on the result.

        :param timeout: Number of seconds to wait before returning None if no
            task is available in the queue. Wait forever if timeout is None
            (the default value).
        :returns: A serialized task message or None if timeout was reached
            before a task arrived.
        """
        raise NotImplementedError('abstract method')

    def __iter__(self):
        """Return an iterator that yields task messages.

        Task iteration blocks when there are no pending tasks to execute.
        """
        while True:
            yield self.get()

    def discard_pending(self):
        """Discard pending tasks from queue"""
        raise NotImplementedError('abstract method')

    def deferred_result(self, task_id):
        """Return a DeferredResult object for the given task id"""
        return DeferredResult(self, task_id)

    def pop(self, task_id, timeout=0):
        """Pop and deserialize the result object for the given task id

        :param task_id: Unique task identifier string.
        :param timeout: Length of time to wait for the result. The default
            behavior is to return immediately (no wait). Wait indefinitely
            if None (dangerous).
        :returns: The deserialized result object.
        :raises: KeyError if the result was not available.
        """
        if timeout < 0:
            raise ValueError('negative timeout not supported')
        message = self.pop_result(task_id, timeout)
        if message is None:
            raise KeyError(task_id)
        return loads(message)

    def set_result(self, task_id, message, timeout):
        """Persist serialized result message.

        :param task_id: Unique task identifier string.
        :param message: Serialized result message.
        :param timeout: Number of seconds to persist the result before
            discarding it.
        """
        raise NotImplementedError('abstract method')

    def pop_result(self, task_id, timeout):
        """Pop serialized result message from persistent storage.

        :param task_id: Unique task identifier string.
        :param timeout: Length of time to wait for the result. Wait indefinitely
            if None. Return immediately if timeout is zero (0).
        :returns: The result message; None if not found.
        """
        raise NotImplementedError('abstract method')

    def update(self, taskset_id, num_tasks, message, timeout):
        """Update the result set for a task set, return all results if complete

        This operation is atomic, meaning that only one caller will ever be
        returned a value other than None for a given `taskset_id`.

        :param taskset_id: (string) The taskset unique identifier.
        :param num_tasks: (int) Number of tasks in the set.
        :param message: (string) A serialized result object to add to the
            set of results.
        :param timeout: (int) Discard results after this number of seconds.
        :returns: None if the number of updates has not reached num_tasks.
            Otherwise return an unordered list of serialized result messages.
        """
        raise NotImplementedError('abstract method')


class _StopWorker(BaseException): pass

def _stop_task():
    raise _StopWorker()
_stop_task.name = '<stop_task>'
