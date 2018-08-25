#!/usr/bin/env python
# encoding: utf-8

from fabric.api import *
from conf.conf import (SPARK_USER, SPARK_HOST, SPARK_USER_PWD, WORK_DIR, WORK_ZIP, REMOTE_WORK_DIR, WORK_PROJ,
                       PY_SETUP, REMOTE_DIR, RAW_DATA_ROOT_WITH_DATE)

import os

from functools import partial
try:
    import cStringIO as StringIO
except:
    import StringIO


# 固定一些参数
run = partial(run, pty=False, timeout=60)


def exec_wrapper(f, host=SPARK_HOST):
    def wrapper(*args, **kwargs):
        rst = execute(f, *args, **kwargs)
        return rst.get(host)

    return wrapper


class FabricInit(object):

    def __init__(self):
        self.set_env()

    @staticmethod
    def set_env():
        env.user = SPARK_USER
        env.hosts = [SPARK_HOST]
        env.password = SPARK_USER_PWD


class FabricHdfsUtils(FabricInit):
    @exec_wrapper
    def hdfs_exits(self, p):
        cmd_format = "hadoop fs -test -e %s" % p
        # cmd 返回值为0 表示存在
        return_code = run(cmd_format, warn_only=True).return_code
        is_exist = True if return_code == 0 else False
        return is_exist

    @exec_wrapper
    def hdfs_mkdir(self, p):
        cmd_format = "hadoop fs -mkdir -p %s" % p
        return run(cmd_format)

    @exec_wrapper
    def hdfs_rmr(self, p):
        cmd_format = "hadoop fs -rmr %s" % p
        return run(cmd_format)

    @exec_wrapper
    def hdfs_mv(self, raw, new):
        cmd_format = "hadoop fs -mv %s %s" % (raw, new)
        return run(cmd_format)

    @exec_wrapper
    def hdfs_put(self, raw, hdfs):
        cmd_format = "hadoop fs -put %s %s" % (raw, hdfs)
        return run(cmd_format, timeout=60*30)

    @exec_wrapper
    def hdfs_getmerge(self, hdfs, fs):
        cmd_format = "hadoop fs -getmerge %s %s" % (hdfs, fs)
        return run(cmd_format)

    @exec_wrapper
    def hdfs_du_sh(self, hdfs, fs):
        cmd_format = "hadoop fs -du -s -h %s %s" % (hdfs, fs)
        # 从结果读回std返回值 todo
        return run(cmd_format)

    @exec_wrapper
    def spark_submit(self, file_name, master="spark://sc-bd-10:7077", executor_memory="20G", total_executor_cores=8,
                     driver_memory="4G"):
        cmd_format = """spark-submit \
        --master {master} \
        --executor-memory {executor_memory} \
        --driver-memory {driver_memory} \
        --total-executor-cores {total_executor_cores} \
        {file_name}""".format(
            master=master, executor_memory=executor_memory,
            driver_memory=driver_memory,
            total_executor_cores=total_executor_cores,
            file_name=file_name,
        )
        # print(cmd_format)
        with cd(REMOTE_WORK_DIR):
            run("chmod 777 *.py")
            return run(cmd_format, timeout=None)


class FabricDbUtils(FabricInit):
    @staticmethod
    @exec_wrapper
    def export_raw_data(cmd):
        with cd(RAW_DATA_ROOT_WITH_DATE):
            run(cmd, timeout=36000)

    @staticmethod
    @exec_wrapper
    def export_all_raw_data_by_sh():
        with cd(RAW_DATA_ROOT_WITH_DATE):
            run("bash %s" % (os.path.join(WORK_DIR, "biz", "export_sql", "export_sql.sh")), timeout=36000)


class Deploy(FabricInit):

    @staticmethod
    @exec_wrapper
    def local_tar():
        local("cd  %s" % WORK_DIR)
        local("rm -rf %s" % WORK_ZIP)
        local("tar -czf %s.tar.gz %s/* --exclude-vcs --exclude=*.tar.gz --exclude=py_log --exclude=dist" % (WORK_PROJ, "."))

    @staticmethod
    @exec_wrapper
    def remote_untar():
        REMOTE_ZIP_PREFIX = os.path.join(REMOTE_DIR, WORK_PROJ)
        with cd(REMOTE_WORK_DIR):
            sudo("tar -zmxf %s.tar.gz" % REMOTE_ZIP_PREFIX)
            sudo("chown -R scdata:scdata %s " % REMOTE_DIR)
            # sudo("chmod -R 777 %s " % REMOTE_DIR)
            # run("rm -rf %s.tar.gz" % REMOTE_ZIP)

    @staticmethod
    @exec_wrapper
    def remote_rm_work_dir():
        sudo("rm -rf %s" % REMOTE_WORK_DIR)

    @staticmethod
    @exec_wrapper
    def remote_mkdir_work_dir():
        run("mkdir -p %s" % REMOTE_WORK_DIR)

    @exec_wrapper
    def setup_py_env(self):
        sudo("pip uninstall %s -y" % PY_SETUP, warn_only=True)
        with cd(REMOTE_WORK_DIR):
            sudo("python setup.py install ")

    @exec_wrapper
    def deploy(self, is_setup=False):
        self.local_tar()
        self.remote_rm_work_dir()
        self.remote_mkdir_work_dir()
        put(WORK_ZIP, REMOTE_DIR)
        self.remote_untar()
        if is_setup:
            self.setup_py_env()

    @exec_wrapper
    def run_submit_task(self, task_name, executor_memory="40G", total_executor_cores=16):
        FabricHdfsUtils().spark_submit(REMOTE_WORK_DIR + "/%s" % task_name, executor_memory=executor_memory, total_executor_cores=total_executor_cores)


