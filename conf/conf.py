#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import uuid
import socket
import getpass
try:
    from sub.deploy_user import DEVELOP_USER
except ImportError:
    DEVELOP_USER = getpass.getuser()

reload(sys)
sys.setdefaultencoding('utf8')


# 工作目录
WORK_DIR = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))
_, WORK_PROJ = os.path.split(WORK_DIR)
WORK_ZIP = os.path.join(WORK_DIR, "%s.tar.gz" % WORK_PROJ)

# 远程
REMOTE_DIR = "/data3/riskModelCodeSubmit/autoSubmitWorkspace/%s/work-for-model-feature-engine" % DEVELOP_USER
REMOTE_WORK_DIR = os.path.join(REMOTE_DIR, WORK_PROJ)

# 安装
PY_SETUP = "sc_risk_model_clean_utils"

# spark run env
SPARK_USER = "huangyu"
SPARK_USER_PWD = "pwd"
SPARK_HOST = "127.0.0.1"


DATE = "2018-07-24"

# 从RDBMS和ES原始csv数据文件的根目录
RAW_DATA_ROOT = "/data3/rawData/riskModelAuto"
RAW_DATA_ROOT_WITH_DATE = os.path.join(RAW_DATA_ROOT, DATE)


# 原始数据hdfs文件的根目录
HDFS_FS_PREFIX = "hdfs://%s:9000" % SPARK_HOST

HDFS_RISK_MODEL_AUTO_ROOT = "/hdfs/riskModelAuto"
HDFS_RISK_MODEL_AUTO_ROOT_WITH_DATE = os.path.join(HDFS_RISK_MODEL_AUTO_ROOT, DATE)
# hdfs原始数据
HDFS_RAW_PREFIX = "raw"
HDFS_RISK_MODEL_AUTO_RAW = os.path.join(HDFS_RISK_MODEL_AUTO_ROOT_WITH_DATE, HDFS_RAW_PREFIX)
# hdfs中间结果数据
HDFS_MID_PREFIX = "mid"
HDFS_RISK_MODEL_AUTO_MID = os.path.join(HDFS_RISK_MODEL_AUTO_ROOT_WITH_DATE, HDFS_MID_PREFIX)

# hdfs feature结果数据
HDFS_FEATURE_PREFIX = "feature"
HDFS_RISK_MODEL_AUTO_FEATURE = os.path.join(HDFS_RISK_MODEL_AUTO_ROOT_WITH_DATE, HDFS_FEATURE_PREFIX)

# 结果数据
RESULT_DATA_ROOT = "/data3/resultData/riskModelAuto"
RESULT_DATA_ROOT_WITH_DATA = os.path.join(RESULT_DATA_ROOT, DATE)


# 原始数据：工商部分，由于不需要每天更新所以原始数据不放在Auto里面，可以直接用
SAIC_NETWORK_2017 = "/data3/rawData/riskModelNotDaily/saicNetwork2017"
# hdfs
HDFS_RISK_MODEL_NOT_DAILY_ROOT = "/hdfs/riskModelNotDaily"
HDFS_RISK_MODEL_NOT_DAILY_RAW = os.path.join(HDFS_RISK_MODEL_NOT_DAILY_ROOT, HDFS_RAW_PREFIX)

HDFS_RISK_MODEL_NOT_DAILY_MID = os.path.join(HDFS_RISK_MODEL_NOT_DAILY_ROOT, HDFS_MID_PREFIX)
HDFS_RISK_MODEL_NOT_DAILY_FEATURE = os.path.join(HDFS_RISK_MODEL_NOT_DAILY_ROOT, HDFS_FEATURE_PREFIX)

# 工商hdfs
HDFS_RISK_MODEL_NOT_DAILY_RAW_SAIC = os.path.join(HDFS_RISK_MODEL_NOT_DAILY_RAW, "saic")
HDFS_RISK_MODEL_NOT_DAILY_MID_SAIC = os.path.join(HDFS_RISK_MODEL_NOT_DAILY_MID, "saic")

# hdfs 统计结果（主要是一些临时统计结果和尝试，还没有到成熟的特征的临时目录）
HDFS_STATISTIC_PREFIX = "statistic"
HDFS_RISK_MODEL_ROOT = "/hdfs/tmp"
HDFS_RISK_MODEL_STATISTIC_TMP = os.path.join(HDFS_RISK_MODEL_ROOT, HDFS_STATISTIC_PREFIX)

# 提交spark server
SPARK_MASTER_URL = "spark://%s:7077" % SPARK_HOST
SPARK_TASK_NAME = "riskModelAuto-%s-%s-%s" % (DATE, uuid.uuid4().hex.upper(), DEVELOP_USER)

# DAG
DAG_SAVE_NAME = os.path.join(WORK_DIR, "dag")
MARKDOWN_SAVE_NAME = os.path.join(WORK_DIR, "doc", "tableInfo.MD")


class RawFiles(object):
    """
    原始文件名
    """

    # 每天需要更新的文件
    DAILY_FILES = [

    ]





