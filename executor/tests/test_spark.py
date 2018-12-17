# coding: utf-8
import unittest

import time
from fwexecutor.context import ExecutionContext
from executor.spark import SparkExecutor


def echo(item):
    print(item)
    time.sleep(3)
    return "done {}".format(item)


items = ["item-{}".format(i) for i in range(1, 21)]


class TestSpark(unittest.TestCase):

    def test_base(self):
        ctx = ExecutionContext()
        ctx.with_kv('spark_app_name', 'my test app')
        ctx.with_kv('spark_url_master', 'local[*]')
        ctx.with_kv('spark.task.cpus', 2)
        ctx.with_kv('spark.default.parallelism', 4)
        ctx.with_kv('spark.python.worker.memory', 1)
        spark_executor = SparkExecutor(ctx)
        print(spark_executor.execute(echo, items))

    def test_context_standard(self):
        ctx = ExecutionContext()
        ctx.with_kv('spark_app_name', 'my test app')
        ctx.with_kv('spark_url_master', 'local[*]')
        ctx.with_kv('spark.task.cpus', 2)
        ctx.with_kv('spark.default.parallelism', 4)
        ctx.with_kv('spark.python.worker.memory', 1)
        spark_executor = SparkExecutor(ctx)
        conf = spark_executor._get_spark_conf()
        self.assertEqual('my test app', conf.get('spark.app.name'))
        self.assertEqual('local[*]', conf.get('spark.master'))
        self.assertEqual('2', conf.get('spark.task.cpus'))
        self.assertEqual('1', conf.get('spark.python.worker.memory'))
        self.assertIsNone(conf.get('unset_property'))
