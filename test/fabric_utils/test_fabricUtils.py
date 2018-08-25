#!/usr/bin/env python
# encoding: utf-8

from unittest import TestCase
from common.utils.fabric_utils import FabricHdfsUtils
from test.test_utils import TestUtils


class TestFabricUtils(TestCase):
    def test_hdfs_exits(self):
        p = "/test/text"
        TestUtils.assert_false(FabricHdfsUtils().hdfs_exits(p), "判断目录:%s 是否存在" % p)

    def test_hdfs_mkdir(self):
        p = "/test/text"
        rst = FabricHdfsUtils().hdfs_mkdir(p)
        print("rst", rst)
        TestUtils.assert_equal(0, rst.return_code, "创建目录: %s" % p)

    def test_hdfs_rmr(self):
        p = "/test/text"
        # 如果不存在, 不存在，新建
        if not FabricHdfsUtils().hdfs_exits(p):
            FabricHdfsUtils().hdfs_mkdir(p)
        a= FabricHdfsUtils().hdfs_rmr(p).return_code

        # 删除
        TestUtils.assert_equal(0, FabricHdfsUtils().hdfs_rmr(p).return_code, "删除目录: %s" % p)

        # 是否删除成功
        TestUtils.assert_false(FabricHdfsUtils().hdfs_exits(p), "判断目录:%s 是否存在" % p)

    def test_spark_submit(self):
        FabricHdfsUtils().spark_submit("./aagsgsfhsg.py")
