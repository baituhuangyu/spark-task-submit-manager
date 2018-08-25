#!/usr/bin/env python
# encoding: utf-8

from conf.all_task_conf import ALL_SPARK_CLASS_TASK
from conf.conf import HDFS_RISK_MODEL_AUTO_RAW
from controller.oslo_utils.importutils import import_class
from common.task_helper.dag import TaskDag
from common.utils.fabric_utils import FabricHdfsUtils, FabricDbUtils
from scpy.logger import get_logger
from biz.load_raw_data.sub.load_data_to_hdfs import LoadRawData
import json

logger = get_logger(__file__)


class Controller(object):
    """
    控制层
    负责 查看 执行spark task class 里面的那些表存在那些表不存在
    生成计算图，调度计算过程
    """
    def __init__(self):
        self.task_dag = TaskDag()
        self.cls_map = {}
        self._task_run_serial = []
        self._task_run_serial_edg = []
        self.fabric_hdfs_utils = FabricHdfsUtils()

    def __import_cls_util(self, cls_name=None, is_single=False):
        for cls_dict in ALL_SPARK_CLASS_TASK:
            cls_str = cls_dict.get("cls_name")
            if is_single and cls_name and cls_str != cls_name:
                continue
            try:
                this_cls = import_class(cls_str)
            except ImportError as e:
                logger.info(cls_str)
                raise e
            self.cls_map[cls_str] = this_cls
            a_node_dag = getattr(this_cls(), "get_spark_task")()
            depend_tables = a_node_dag["depend_tables"]
            result_tables = a_node_dag["result_tables"]
            # 构建dag, 添加节点
            self.task_dag.add_nodes(depend_tables+result_tables)
            # 构建dag, 添加边
            self.task_dag.add_dag(cls_dict, depend_tables, result_tables)

    def __import_all_class(self):
        self.__import_cls_util()

    def __import_single_class(self, cls_name):
        self.__import_cls_util(cls_name=cls_name, is_single=True)

    def plot(self):
        self.__import_all_class()
        self.analyse()
        self.task_dag.plot(view=True)
        self.task_dag.markdown()

    def analyse(self):
        # 查看那个表计算是存在的那个表是不存在的。
        # self.task_dag.set_table_info(self.fabric_hdfs_utils.hdfs_exits)
        # 做bfs
        to_left_link, self._task_run_serial_edg, self._task_run_serial = self.task_dag.bfs()

        to_left_tables = [_["target"] for _ in to_left_link]
        logger.info("to_left_tables:\n" + json.dumps(to_left_tables, ensure_ascii=False, indent=4))
        logger.info("_task_run_serial:\n" + json.dumps(self._task_run_serial, ensure_ascii=False, indent=4))

    def run_all(self):
        self.__import_all_class()
        self.analyse()

        for task_dict in self._task_run_serial:
            cls_name = task_dict.get("cls_name")
            if task_dict.get("need_to_run"):
                task = self.cls_map[cls_name]
                logger.info("task class %s starts" % cls_name)
                getattr(task(), "run_task")()
                logger.info("task class %s done" % cls_name)

    def run_single(self, cls_name):
        self.__import_single_class(cls_name)
        # self.analyse()
        task = self.cls_map[cls_name]
        getattr(task(), "run_task")()

    def load_not_exit(self):
        pass

    @staticmethod
    def reload_all_daily_hdfs():
        fabric_hdfs_utils = FabricHdfsUtils()
        if fabric_hdfs_utils.hdfs_exits(HDFS_RISK_MODEL_AUTO_RAW):
            fabric_hdfs_utils.hdfs_rmr(HDFS_RISK_MODEL_AUTO_RAW)
        fabric_hdfs_utils.hdfs_mkdir(HDFS_RISK_MODEL_AUTO_RAW)
        LoadRawData().put_all_daily()

    @staticmethod
    def export_raw_data():
        FabricDbUtils().export_all_raw_data_by_sh()

    def collect(self):
        """
        搜集
        """
        pass

    def save_all(self):
        """
        保存所有数据
        :return:
        """
        pass



