# coding: utf-8
import logging
import sys
from fwexecutor.executors.base import BaseExecutor
from fwexecutor.context import ExecutionContext
from pyspark import SparkConf
from pyspark import SparkContext

logger = logging.getLogger('executor.spark')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


class SparkExecutor(BaseExecutor):
    
    def __init__(self, context=None):
        if context is None:
            raise ValueError('Context is mandatory for SparkExecutor')
        self._context = context

    def execute(self, func, items):
        try:
            logger.info('In spark executor')

            conf = SparkConf().setAppName(self._context['spark_app_name']) \
                .setMaster(self._context['spark_url_master']) \
                .set("spark.task.cpus", self._context.get('spark.task.cpus', 16)) \
                .set("spark.default.parallelism",
                     self._context.get('spark.default.parallelism', 16)) \
                .set("spark.python.worker.memory",
                     self._context.get('spark.python.worker.memory', '50g')) \
                .set("spark.task.maxFailures", self._context.get('spark.task.maxFailures', 1))
            sc = SparkContext(conf=conf)

            rv = sc.parallelize(items).map(func).collect()
            sc.stop()

            return rv
        finally:
            logger.info('Spark executor done')
