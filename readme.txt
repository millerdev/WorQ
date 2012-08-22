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
- Fix TODO items in worq.pool.process
    - Add support for heartbeat/keepalive
      Atomically set result timeout when task processing begins
x       - Refactor/simplify broker to manage a single queue (for BRPOPLPUSH)
        - Combine queue and result store (they need to interact)
            can always make a hybrid (ex: Redis/Postgres) backend if needed

        BRPOPLPUSH next task id
        atomically:
            EXPIRE result
            GET task details
            LREM task id from queue (process task if successful)

    - Improve status/heartbeat handling to not process old status values.

- Include task name in repr of DeferredResult

- Improve task serialization for fast option and task_id access (avoid unpickle of parameters, etc.)
- Come up with a name for the worker pool coordinator process.
- TaskSet should store final task with its results
- Call taskset with no args uses identity task that simply returns it's first arg
- DeferredResult.wait should continue waiting if its value is a DeferredResult
    - DeferredResult should be picklable
- ?Add dependent task to a DeferredResult (value of deferred is passed to dependent)
- Reload worker pool config on HUP
- Task monitoring (must be optional)
    - update result heartbeat periodically
    - use this for better TaskSet resilience
- Guaranteed message delivery in redis
    - result is created in redis on enqueue task
    - task/result has a state machine: pending, in process, completed, lost...
    - running task can update its status on its (in process) result object
- Skip tests if queue backend is not running

x Pass TaskStatus objects through result queue (avoid extra status key)
x MIT license
x Move worq.procpool to worq.pool.process
x new name for project: WorQ
x Worker process pool
x   - Controlled worker shutdown without loss of work
xx Use multiprocessing.ProcessPool (look into process monitoring, dying, etc.)
x Make task.wait block on queue result with timeout (use queue primitives rather than busy wait)
