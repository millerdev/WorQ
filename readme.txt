TODO
x Worker process pool
x   - Controlled worker shutdown without loss of work
- Use multiprocessing.ProcessPool (look into process monitoring, dying, etc.)
- Improve task serialization for fast option and task_id access (avoid unpickle of parameters, etc.)
- Make task.wait block on queue result with timeout (use queue primitives rather than busy wait)
- Reload worker pool config on HUP
- Task monitoring (must be optional)
    - update result heartbeat periodically
    - use this for better TaskSet resilience
- Guaranteed message delivery in redis
    - result is created in redis on enqueue task
    - task/result has a state machine: pending, in process, completed, lost...
    - running task can update its status on its (in process) result object
