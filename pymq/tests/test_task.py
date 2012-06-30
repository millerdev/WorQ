from pymq import get_broker, queue, Task, TaskSet, TaskFailure, TaskSpace
from pymq.tests.util import (assert_raises, eq_, eventually, thread_worker,
    with_urls)

@with_urls
def test_TaskSet_on_error_FAIL(url):

    def func(arg):
        if arg == 0:
            raise Exception('zero fail!')
        return arg

    broker = get_broker(url)
    broker.expose(func)
    with thread_worker(broker):

        # -- task-invoking code, usually another process --
        q = queue(url)

        tasks = TaskSet(result_timeout=5)
        tasks.add(q.func, 1)
        tasks.add(q.func, 0)
        tasks.add(q.func, 2)
        res = tasks(q.func)
        res.wait(timeout=1, poll_interval=0)

        with assert_raises(TaskFailure,
                'func [default:%s] subtask(s) failed' % res.id):
            res.value
