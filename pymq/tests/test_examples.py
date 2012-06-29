def example(func):
    # test helper
    example.s.append(func)
    return func
example.s = []

def test_examples():
    import examples
    for url in [
        'memory://',
        'redis://localhost:16379/0', # NOTE non-standard port
    ]:
        for test in example.s:
            yield test, url
