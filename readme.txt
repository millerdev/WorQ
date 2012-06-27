TODO
- Task heartbeat for monitoring
    - use this for better TaskSet resilience
- Guaranteed message delivery in redis (maybe)
    - Make atomic operation to...
        - pop task id from normal queue
        - abort if "processing"
        - set "processing" (with timeout?)
        - push task id to "processing" queue
- Worker pool
