#!/usr/bin/env python
# encoding: utf-8

from common.utils.fabric_utils import FabricHdfsUtils
from conf.conf import RAW_DATA_ROOT_WITH_DATE, HDFS_RISK_MODEL_AUTO_RAW
from conf.conf import RawFiles
import os


class LoadRawData(object):
    fabric_utils = FabricHdfsUtils()
    raw_file_folder = RawFiles()

    def put_utils(self, file_name):
        raw = os.path.join(RAW_DATA_ROOT_WITH_DATE, file_name)
        hdfs = os.path.join(HDFS_RISK_MODEL_AUTO_RAW, file_name)
        self.fabric_utils.hdfs_put(raw, hdfs)

    def put_all_daily(self):
        map(lambda file_name: self.put_utils(file_name), self.raw_file_folder.DAILY_FILES)


