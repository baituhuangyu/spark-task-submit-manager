#!/usr/bin/env python
# encoding: utf-8


from conf.conf import SPARK_MASTER_URL, SPARK_TASK_NAME
from pyspark.sql import SparkSession
from common.task_helper.dag import SparkTask
import abc


class SparkSessionUtils(SparkTask):

    session = SparkSession.builder \
        .master(SPARK_MASTER_URL) \
        .appName(SPARK_TASK_NAME) \
        .getOrCreate()

    session.conf.set("spark.driver.maxResultSize", "4g")
    session.conf.set("spark.sql.broadcastTimeout", 1200)
    session.conf.set("spark.sql.crossJoin.enabled", "true")
    # session.sparkContext.addPyFile(WORK_ZIP)

    # def add_zip_py(self):
    #     self.session.sparkContext.addPyFile(WORK_ZIP)

    @abc.abstractmethod
    def run_task(self):
        raise NotImplementedError

    def _run_task(self):
        # self.add_zip_py()
        self.run_task()
        self.session.stop()









