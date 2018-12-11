# coding: utf-8
import logging
import sys
from fwexecutor.executors.base import BaseExecutor
from fwexecutor.context import ExecutionContext

logger = logging.getLogger('executor.spark')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


class SparkExecutor(BaseExecutor):
    
    def __init__(self, context=None):
        self._context = context or ExecutionContext()

    def execute(self, func, items):
        try:
            logger.info('in "SPARK" executor')
            rv = []
            for item in items:
                logger.info('Executing {} on {}'.format(func.__name__, item))
                rv.append(func(item))
            return rv
        finally:
            logger.info('"SPARK" executor done')
