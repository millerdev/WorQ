
========================
WorQ - Python task queue
========================

WorQ is a Python task queue that uses a worker pool to execute tasks in
parallel. Workers can run in a single process, multiple processes on a single
machine, or many processes on many machines. It ships with two backend options
(memory and redis) and two worker pool implementations (multi-process and
threaded). Task results can be monitored, waited on, or passed as arguments to
another task.

WorQ has two main components:

* ``TaskQueue``
* ``WorkerPool``

WorQ ships with more than one implementation of each of these components.

* ``worq.queue.memory.TaskQueue`` - an in-memory (process local) task queue.

* ``worq.queue.redis.TaskQueue`` - a Redis-backed task queue that can scale
  to multiple servers.

* ``worq.pool.thread.WorkerPool`` - a multi-thread worker pool.

* ``worq.pool.process.WorkerPool`` - a multi-process worker pool.

These components can be mixed and matched as desired to meet the needs of your
application. For example, an in-memory task queue can be used with a multi-
process worker pool to to execute truely concurrent Python tasks on a single
multi-core machine.


An example with Redis and a multi-process worker pool
=====================================================

Create the following files.

``tasks.py``::

    import logging
    from worq import get_broker, TaskSpace

    ts = TaskSpace(__name__)

    def init(url):
        logging.basicConfig(level=logging.DEBUG)
        broker = get_broker(url)
        broker.expose(ts)
        return broker

    @ts.task
    def num(value):
        return int(value)

    @ts.task
    def add(values):
        return sum(values)

``pool.py``::

    #!/usr/bin/env python
    import sys
    from worq.pool.process import WorkerPool
    from tasks import init

    def main(url, **kw):
        broker = init(url)
        pool = WorkerPool(broker, init, workers=2)
        pool.start(**kw)
        return pool

    if __name__ == '__main__':
        main(sys.argv[-1])

``main.py``::

    #!/usr/bin/env python
    import sys
    import logging
    from worq import get_queue

    def main(url):
        logging.basicConfig(level=logging.DEBUG)
        q = get_queue(url)

        # enqueue tasks to be executed in parallel
        nums = [q.tasks.num(x) for x in range(10)]

        # process the results when they are ready
        result = q.tasks.add(nums)

        # wait for the final result
        result.wait(timeout=30)

        print('0 + 1 + ... + 9 = {}'.format(result.value))

    if __name__ == '__main__':
        main(sys.argv[-1])

Make sure Redis is accepting connections on port 6379. It is recommended, but
not required, that you setup a virtualenv. Then, in a terminal window::

    $ pip install "WorQ[redis]"
    $ python pool.py redis://localhost:6379/0

And in a second terminal window::

    $ python main.py redis://localhost:6379/0

Tasks may also be queued in in memory rather than using Redis. In this case
the queue must reside in the same process that initiates tasks, but the work
can still be done in separate processes. For example:


Example with memory queue and a multi-process worker pool
=========================================================

In addition to the three files from the previous example, create the following:

``mem.py``::

    #!/usr/bin/env python
    import main
    import pool

    if __name__ == "__main__":
        url = "memory://"
        p = pool.main(url, timeout=2, handle_sigterm=False)
        try:
            main.main(url)
        finally:
            p.stop()

Then, in a terminal window::

    $ python mem.py


See :ref:`examples.py` for more things that can be done with WorQ.


Links
=====

* Documentation: http://worq.readthedocs.org/
* Source: https://github.com/millerdev/WorQ/
* PyPI: http://pypi.python.org/pypi/WorQ


Running the tests
=================

WorQ development is mostly done using TDD. Tests are important to verify that
new code works. You may want to run the tests if you want to contribute to WorQ
or simply just want to hack. Setup a virtualenv and run these commands where you
have checked out the WorQ source code::

    $ pip install nose
    $ nosetests

The tests for some components (e.g., redis TaskQueue) are disabled unless
the necessary requirements are available. For example, by default the tests
look for redis at ``redis://localhost:16379/0`` (note non-standard port; you
may customize this url with the ``WORQ_TEST_REDIS_URL`` environment variable).


==========
Change Log
==========

v1.1.1, 2018-03-20
  - Add example using memory queue
  - Fix python 3 compatibility

v1.1.0, 2014-03-29
  - Add support for Python 3

v1.0.2, 2012-09-07
  - Allow clearing entire Queue with ``del queue[:]``.
  - Raise ``DuplicateTask`` (rather than the more generic ``TaskFailure``) when
    trying to enqueue a task with an id matching that of another task in the
    queue.

v1.0.1, 2012-09-06
  - Better support for managing more than one process.WorkerPool with a single
    pool manager process.
  - Queue can be created with default task options.
  - Can now check the approximate number of tasks in the queue with len(queue).
  - Allow passing a completed Deferred as an argument to another task.
  - Fix redis leaks.

v1.0.0, 2012-09-02 -- Initial release.

