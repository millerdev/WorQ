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
from threading import Event, Thread
from uuid import uuid4
from worq.task import TaskSpace

log = logging.getLogger(__name__)


class WorkerPool(object):
    """Multi-thread worker pool

    :param broker: Queue broker instance.
    :param workers: Number of workers in the pool.
    :param thread_factory: A factory function that creates a new thread
        object. This should have the same signature as
        ``threading.Thread`` and should return a thread object.
    """

    def __init__(self, broker, workers=1, thread_factory=Thread):
        self.broker = broker
        self.workers = workers
        self.thread_factory = thread_factory
        self.threads = []
        self.stop_event = Event()
        ts = TaskSpace(__name__)
        ts.task(lambda:None, 'noop')
        broker.expose(ts, replace=True)

    def start(self, timeout=1):
        """Start worker threads.

        :param timeout: The number of seconds to wait for a task before
            checking if the pool has been asked to stop.
        """
        self.stop_event.clear()
        for ident in xrange(self.workers):
            args = (ident, self.broker, timeout, self.stop_event)
            thread = self.thread_factory(target=worker, args=args)
            thread.start()
            self.threads.append(thread)

    def stop(self, use_sentinel=False, join=True):
        """Stop the worker pool

        :param use_sentinel: Enqueue a no-op task for each worker if true.
            This will result in a more responsive shutdown if there are no
            other worker pools consuming tasks from the broker.
        :param join: Join each thread afer sending the stop signal.
        """
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        if use_sentinel:
            q = self.broker.queue(__name__)
            for thread in self.threads:
                if thread.is_alive():
                    q.noop()
        if join:
            self.join()

    def join(self):
        """Wait for all threads to stop (call ``stop(join=False)`` first)"""
        for thread in self.threads:
            thread.join()


def worker(ident, broker, timeout, stop_event):
    try:
        while not stop_event.is_set():
            task = broker.next_task(timeout=timeout)
            if task is not None:
                broker.invoke(task)
    except:
        log.error('worker %s crashed', ident, exc_info=True)
