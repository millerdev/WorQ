from worq.tests.util import TEST_URLS

def example(func):
    # test helper
    example.s.append(func)
    return func
example.s = []

def test_examples():
    import examples
    for url in TEST_URLS:
        for test in example.s:
            yield test, url
