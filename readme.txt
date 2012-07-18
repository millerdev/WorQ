TODO
x Worker process pool
x   - Controlled worker shutdown without loss of work
xx Use multiprocessing.ProcessPool (look into process monitoring, dying, etc.)
x Make task.wait block on queue result with timeout (use queue primitives rather than busy wait)
- Fix TODO items in procpool
- Move pymq.procpool to pymq.pool.process
- Include task name in repr of DeferredResult
- Pass TaskStatus objects through result queue (avoid extra status key)
- Improve task serialization for fast option and task_id access (avoid unpickle of parameters, etc.)
- Come up with a name for the worker pool coordinator process.
- Reload worker pool config on HUP
- Task monitoring (must be optional)
    - update result heartbeat periodically
    - use this for better TaskSet resilience
- Guaranteed message delivery in redis
    - result is created in redis on enqueue task
    - task/result has a state machine: pending, in process, completed, lost...
    - running task can update its status on its (in process) result object
