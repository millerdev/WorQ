import logging
from collections import defaultdict
from pickle import dumps, loads
from weakref import ref as weakref

from pymq.const import DEFAULT
from pymq.task import Queue, TaskSet, TaskSpace, TaskFailure, DeferredResult

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

    def __init__(self, message_queue, result_store):
        self.messages = message_queue
        self.results = result_store
        self.tasks = {_stop_task.name: _stop_task}

    def expose(self, obj):
        """Expose a TaskSpace or task callable to all queues.

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
            for queue, message in self.messages:
                self.invoke(queue, message)
        except _StopWorker:
            log.info('worker stopped')

    def stop(self):
        """Stop a random worker.

        WARNING this is only meant for testing purposes. It will likely not do
        what you expect in an environment with more than one worker.
        """
        queue = self.messages.stop_queue
        self.enqueue(queue, 'stop', _stop_task.name, (), {}, {})

    def discard_pending_tasks(self):
        """Discard pending tasks from all queues"""
        self.messages.discard_pending()

    def queue(self, queue=DEFAULT, target=''):
        return Queue(self, queue, target)

    def enqueue(self, queue, task_id, task_name, args, kw, options):
        unknown_options = set(options) - self.task_options
        if unknown_options:
            raise ValueError('unrecognized task options: %s'
                % ', '.join(unknown_options))
        log.debug('enqueue %s [%s:%s]', task_name, queue, task_id)
        message = dumps((task_id, task_name, args, kw, options))
        result_status = options.get('result_status', False)
        if result_status or 'result_timeout' in options:
            result = self.results.deferred_result(task_id)
            if result_status:
                timeout = options.get('result_timeout', DAY)
                self.results.set_status(task_id, dumps('enqueued'), timeout)
        else:
            result = None
        self.messages.enqueue_task(queue, message)
        return result

    def invoke(self, queue, message):
        try:
            task_id, task_name, args, kw, options = loads(message)
        except Exception:
            log.error('cannot load task message: %s', message, exc_info=True)
            return
        log.debug('invoke %s [%s:%s]', task_name, queue, task_id)
        timeout = options.get('result_timeout', DAY)
        result_status = options.get('result_status', False)
        if result_status:
            self.results.set_status(task_id, dumps('processing'), timeout)
            def update_status(value):
                self.results.set_status(task_id, dumps(value), timeout)
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
                message = dumps([result])
                self.results.set_result(task_id, message, timeout)

    def process_taskset(self, queue, taskset, result):
        taskset_id, task_name, args, kw, options, num = taskset
        timeout = options.get('result_timeout', DAY)
        if (options.get('on_error', TaskSet.FAIL) == TaskSet.FAIL
                and isinstance(result, TaskFailure)):
            # suboptimal: pending tasks in the set will continue to be executed
            # and their results will be persisted if they succeed.
            message = dumps([TaskFailure(
                task_name, queue, taskset_id, 'subtask(s) failed')])
            self.results.set_result(taskset_id, message, timeout)
        else:
            results = self.results.update(
                taskset_id, num, dumps(result), timeout)
            if results is not None:
                args = ([loads(r) for r in results],) + args
                self.enqueue(queue, taskset_id, task_name, args, kw, options)

    def deferred_result(self, task_id):
        return self.results.deferred_result(task_id)


class AbstractMessageQueue(object):
    """Message queue abstract base class

    :param url: URL used to identify the queue.
    :param *queues: One or more strings representing the names of queues to
        listen for during message iteration.
    """

    def __init__(self, url, queues):
        self.url = url
        self.queues = list(queues) if queues else [DEFAULT]

    @property
    def stop_queue(self):
        return self.queues[0]

    def __iter__(self):
        """Return an iterator that yields task messages.

        Task iteration normally blocks when there are no pending tasks to
        execute. Each yielded item must be a two-tuple consisting of
        (<queue name>, <task message>).
        """
        raise NotImplementedError('abstract method')

    def get(self, timeout=None):
        """Get a task message from the queue

        :param timeout: Number of seconds to wait before returning None if no
            task is available in the queue. Wait forever if timeout is None
            (the default value).
        :returns: A task message; None if timeout was reached before a task
            arrived.
        """
        raise NotImplementedError('abstract method')

    def enqueue_task(self, queue, message):
        """Enqueue a task message onto a named task queue.

        :param queue: Queue name.
        :param message: Serialized task message.
        """
        raise NotImplementedError('abstract method')

    def discard_pending(self):
        """Discard pending tasks from all queues"""
        raise NotImplementedError('abstract method')


class AbstractResultStore(object):
    """Result store abstract base class

    :param url: URL used to identify the queue.
    """

    def __init__(self, url):
        self.url = url

    def deferred_result(self, task_id):
        """Return a DeferredResult object for the given task id"""
        return DeferredResult(self, task_id)

    def pop(self, task_id):
        """Pop and deserialize the result object for the given task id"""
        message = self.pop_result(task_id)
        if message is None:
            return None
        return loads(message)

    def status(self, task_id):
        """Pop and deserialize task status

        :param task_id: Unique task identifier string.
        :returns: Deserialized status object.
        :raises: KeyError if there is no status or the status has already
            been popped.
        """
        return loads(self.pop_status(task_id))

    def set_result(self, task_id, message, timeout):
        """Persist serialized result message.

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :param message: Serialized result message.
        :param timeout: Number of seconds to persist the result before
            discarding it.
        """
        raise NotImplementedError('abstract method')

    def pop_result(self, task_id):
        """Pop serialized result message from persistent storage.

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :returns: The result message; None if not found.
        """
        raise NotImplementedError('abstract method')

    def set_status(self, task_id, message, timeout):
        """Persist serialized task status

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :param message: (string) Serialized status object.
        :param timeout: Number of seconds to persist the status before
            discarding it.
        """
        raise NotImplementedError('abstract method')

    def pop_status(self, task_id):
        """Pop serialized task status

        Must be implemented by each broker implementation. Not normally called
        by user code.

        :param task_id: Unique task identifier string.
        :returns: (string) Serialized status object.
        :raises: KeyError if there is no status for the given task id.
        """
        raise NotImplementedError('abstract method')

    def update(self, taskset_id, num_tasks, message, timeout):
        """Update the result set for a task set, return all results if complete

        Must be implemented by each broker implementation. Not normally called
        by user code. This operation is atomic, meaning that only one caller
        will ever be returned a value other than None for a given `taskset_id`.

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
