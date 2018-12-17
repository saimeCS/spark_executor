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

    def _get_spark_conf(self):
        conf = SparkConf()

        for key in self._context.keys():
            if key in 'spark_app_name':
                conf.setAppName(self._context['spark_app_name'])
                continue
            if key in 'spark_url_master':
                conf.setMaster(self._context['spark_url_master'])
                continue
            conf.set(key, self._context.get(key))

        return conf

    def execute(self, func, items):
        try:
            logger.info('In spark executor')

            sc = SparkContext(conf=self._get_spark_conf())

            rv = sc.parallelize(items).map(func).collect()
            sc.stop()

            return rv
        finally:
            logger.info('Spark executor done')
