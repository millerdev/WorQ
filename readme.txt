TODO
- Controlled worker shutdown without loss of work
- Worker process pool
- Task heartbeat for monitoring
    - use this for better TaskSet resilience
    - this can probably be implemented with DeferredResult status updates
- Guaranteed message delivery in redis
    - result is created in redis on enqueue task
    - task/result has a state machine: pending, in process, completed, lost...
    - running task can update its status on its (in process) result object
