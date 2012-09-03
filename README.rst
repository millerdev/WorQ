
========================
WorQ - Python task queue
========================

WorQ is a task queue library written in Python. There are two main components
that work together:

* ``TaskQueue``
* ``WorkerPool``

WorQ ships with more than one implementation of each of these components.

* ``worq.queue.memory.TaskQueue`` - an in-memory (process local) task queue.

* ``worq.queue.redis.TaskQueue`` - a Redis-backed task queue that can scale
  to multiple servers.

* ``worq.pool.thread.WorkerPool`` - a multi-thread worker pool.

* ``worq.pool.process.WorkerPool`` - a multi-process worker pool.

These components can be mixed and matched to meet various needs.


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

    def main(url):
        broker = init(url)
        pool = WorkerPool(broker, init, workers=2)
        pool.start()

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

        print('1 + 2 + ... + 10 = {}'.format(result.value))

    if __name__ == '__main__':
        main(sys.argv[-1])

Make sure Redis is accepting connections on port 6379. It is recommended, but
not required, that you setup a virtualenv. Then, in a terminal window::

    $ pip install "WorQ[redis]"
    $ python pool.py redis://localhost:6379/0

And in a second terminal window::

    $ python main.py redis://localhost:6379/0

See examples.py for more things that can be done with WorQ.


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
look for redis at redis://localhost:16379/0 (note non-standard port; you may
customize this url with the ``WORQ_TEST_REDIS_URL`` environment variable).


==========
Change Log
==========

v1.0, 2012-09-02 -- Initial release.

