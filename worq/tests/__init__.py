import os
from nose.plugins.skip import SkipTest

TEST_URLS = ['memory://']

def setup():
    for get_test_url in [get_redis_url]:
        url = get_test_url()
        if url is not None:
            TEST_URLS.append(url)

def get_redis_url():
    redis_url = os.environ.get(
        'WORQ_TEST_REDIS_URL', 'redis://localhost:16379/0') # non-standard port
    if redis_url != 'disabled':
        try:
            import redis
        except ImportError:
            pass
        else:
            from worq.queue.redis import TaskQueue as RedisQueue
            queue = RedisQueue(redis_url)
            if queue.ping():
                return redis_url
    return None

def test_redis_should_be_installed():
    try:
        import redis
    except ImportError:
        if 'WORQ_TEST_REDIS_URL' in os.environ:
            assert 0, 'WORQ_TEST_REDIS_URL is set but redis is not installed'
        else:
            raise SkipTest(
                'cannot test redis task queue because redis is not installed')
