from setuptools import setup, find_packages
from worq import __version__

setup(
    name='WorQ',
    version=__version__,
    author='Daniel Miller',
    author_email='millerdev@gmail.com',
    packages=find_packages(),
    url='http://worq.readthedocs.org/',
    license='LICENSE.txt',
    description='Python task queue',
    long_description=open('README.rst').read() \
        .replace(':ref:', ''), # HACK replace so pypi doesn't barf on the reST
    extras_require={
        "redis": "redis >= 2.4",
    },
)
