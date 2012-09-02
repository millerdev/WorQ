from setuptools import setup, find_packages

setup(
    name='WorQ',
    version='1.0',
    author='Daniel Miller',
    author_email='millerdev@gmail.com',
    packages=find_packages(),
    url='http://github.com/millerdev/WorQ/',
    license='LICENSE.txt',
    description='Python task queue',
    long_description=open('README.rst').read(),
    extras_require={
        "redis": "redis >= 2.4",
    },
    tests_require=[
        "nose >= 1.0",
    ],
)
