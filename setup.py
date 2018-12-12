# coding: utf-8
import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='spark executor',
    version=read('VERSION'),
    packages=find_packages(),
    entry_points={
        'executors': [
            'spark = executor.spark:SparkExecutor',
        ],
    },
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'pyspark==2.3.2',
        'executor-framework==0.1',
    ],
    dependency_links=['git+https://github.com/saimeCS/fwexecutor.git@0.1#egg=executor-framework-0.1']
)
