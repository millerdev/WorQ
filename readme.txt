WorQ - asynchronous Python task queue.

Copyright (c) 2012 Daniel Miller

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

-------------------------------------------------------------------------------

TODO

- TaskSet should be resilient to lost intermediate results
- What happens to TaskSet results when a subtask is not invoked before the
    result set expires (e.g., when the broker is busy)? This should not happen.
    IOW, TaskSet results should not expire when there are subtasks in the queue
    waiting to be invoked.
- DeferredResult.wait should continue waiting if its value is a DeferredResult
    - DeferredResult should be picklable
- Allow tasks to be cancelled
- Implement "map" and "reduce"
- Decouple TaskSpace from Broker?
- ?Add dependent task to a DeferredResult (value of deferred is passed to dependent)
- Reload worker pool config on HUP
- Skip tests if queue backend is not running


Completed

x - Call taskset with no args uses default task that simply returns it's first arg
x - Ignore None in taskset results
x - TaskSet should store final task with its results
x - Implement thread pool
x - Come up with a name for the worker pool coordinator process. "Pool manager"
x - Guaranteed message delivery in redis
x   - result is created in redis on enqueue task
x   - task/result has a state machine: pending, in process, completed, lost...
x   - running task can update its status on its (in process) result object
x - Task monitoring (must be optional)
x   - update result heartbeat periodically
x   - use this for better TaskSet resilience
xx - Improve task serialization for fast option and task_id access (avoid unpickle of parameters, etc.)
x - Include task name in repr of DeferredResult
x - Fix TODO items in worq.pool.process
x   - Add support for heartbeat/keepalive
x     Atomically set result timeout when task processing begins
x       - Refactor/simplify broker to manage a single queue (for BRPOPLPUSH)
x       - Combine queue and result store (they need to interact)
x           can always make a hybrid (ex: Redis/Postgres) backend if needed
x       BRPOPLPUSH next task id
x       atomically:
x           EXPIRE result
x           GET task details
x           LREM task id from queue (process task if successful)
x   - Improve status/heartbeat handling to not process old status values.
x Pass TaskStatus objects through result queue (avoid extra status key)
x MIT license
x Move worq.procpool to worq.pool.process
x new name for project: WorQ
x Worker process pool
x   - Controlled worker shutdown without loss of work
xx Use multiprocessing.ProcessPool (look into process monitoring, dying, etc.)
x Make task.wait block on queue result with timeout (use queue primitives rather than busy wait)
