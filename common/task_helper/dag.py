#!/usr/bin/env python
# encoding: utf-8

import os
from graphviz import Digraph
from conf.conf import DAG_SAVE_NAME, MARKDOWN_SAVE_NAME
import hashlib
import abc
from pyspark.sql.types import StructType
from collections import OrderedDict


def gen_md5(s):
    """
    s 公司名字 utf8编码
    """
    if not s:
        raise Exception("s cannot be void")

    m = hashlib.md5()
    if isinstance(s, unicode):
        s = s.encode("utf8")
    m.update(s)
    return m.hexdigest().upper()


class TableInfo(object):
    def __init__(self, name, schema, location, meaning, *args, **kwargs):
        if isinstance(name, basestring) and isinstance(schema, StructType) and isinstance(location, basestring)\
                and isinstance(meaning, list) and len(schema.names) == len(meaning):
            self._name, self._schema, self._location = name, schema, location
            self._meaning = self.__parse_meaning(meaning)
        else:
            print(name, schema, location, meaning)
            raise Exception("name: %s, schema: %s, location: %s, meaning: %s, len(schema.names): %s, len(meaning): %s" %
                            (type(name), type(schema), type(location), type(meaning), len(schema.names), len(meaning)))

        self._status, self._cap, self._size = None, None, None

    def __parse_meaning(self, meaning):
        # todo
        return meaning

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = location

    @property
    def meaning(self):
        return self._meaning

    @meaning.setter
    def meaning(self, meaning):
        self._meaning = meaning

    @property
    def meaning_zip(self):
        return zip(self.schema.names, self._meaning)

    @property
    def fields(self):
        return self.schema.names

    @property
    def meaning_dict(self):
        return OrderedDict(zip(self.schema.names, self._meaning))

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def cap(self):
        return self._cap

    @cap.setter
    def cap(self, cap):
        self._cap = cap

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, size):
        self._size = size


# todo
class Node(object):
    def __init__(self, **kwargs):
        node_keys = [
            "_id"
        ]

        for k in node_keys:
            self.__setattr__(k, kwargs.get(k))


class Link(object):
    pass


class TaskDag(object):
    """
    spark dag 流图
    """

    Nodes = []
    _nodes_set = set()
    Links = []
    _links_set = set()


    Network = {
        "nodes": Nodes,
        "links": Links,
    }

    def add_nodes(self, tbs):
        for t in tbs:
            if isinstance(t, TableInfo):
                t_id = gen_md5(t.location)
                if t_id not in self._nodes_set:
                    self.Nodes.append({
                        "id": t_id,
                        "name": t.name,
                        "location": t.location,
                        "schema": t.schema,
                        "meaning": t.meaning,
                        "doc": t.__doc__,
                        # 没想好 todo
                        "tb_cls": t,
                        "status": t.status,
                        "cap": t.cap,
                        "size": t.size,
                    })
                    self._nodes_set.add(t_id)

    def set_table_info(self, fun):
        """
        判断表是否存在
        """
        for node in self.Nodes:
            tb_location = node["location"]
            node["status"] = fun(tb_location)

    def add_dag(self, cls_dict, depend_tables, result_tables):
        cls_name = cls_dict.get("cls_name")
        need_to_run = cls_dict.get("need_to_run")
        for d in depend_tables:
            d_id = gen_md5(d.location)

            for r in result_tables:
                r_id = gen_md5(r.location)
                a_link_id = "".join([d_id, r_id])
                if a_link_id not in self._links_set:
                    a_link = {
                        "id": a_link_id,
                        "source": d_id,
                        "target": r_id,
                        "cls_name": cls_name,
                        "need_to_run": need_to_run,
                    }
                    self.Links.append(a_link)

        self.Network["nodes"] = self.Nodes
        self.Network["links"] = self.Links

    def plot(self, path=DAG_SAVE_NAME, view=False):
        nodes = self.Network.get("nodes", [])
        links = self.Network.get("links", [])
        dot = Digraph(comment='designed by hy', engine="dot",
                      node_attr={'shape': 'plaintext', "root": "true", },
                      edge_attr={"arrowhead": "vee", "root": "true"},
                      )

        for node in nodes:
            node_other_att = {}
            # 未完成的节点为False
            if not node.get("status"):
                node_other_att.update(**{"fillcolor": "red", "style": "filled"})
            # 拼接 label
            location = node.get("location", "")
            schema = node.get("schema")
            meaning = node.get("meaning", [])

            schema_str = schema.simpleString() if schema else ""
            schema_name_type = [sub_str.split(":") for sub_str in schema_str.replace("struct<", "").replace(">", "").split(",")]
            # schema_str_new = "{%s}" % "|<here>".join(["%s" % "|".join(_) for _ in schema_name_type])
            schema_name_type = [v+[meaning[idx]] for (idx, v) in enumerate(schema_name_type)]
            schema_str_new = "{%s}" % "|<here>".join(["&#92;n".join(_) for _ in schema_name_type])

            label = "{%s|%s|%s}" % (location, node.get("doc", "") or "", schema_str_new)
            dot.node(name=node.get("id", ""),
                     label=label,
                     shape="record",
                     **node_other_att)

        for link in links:
            label = link.get("cls_name", "")
            dot.edge(link.get("source"), link.get("target"), label=label)

        dot.render('%s.gv' % path, view=view)

    def markdown(self, path=MARKDOWN_SAVE_NAME):
        nodes = self.Network.get("nodes", [])
        nodes = sorted(nodes, key=lambda _: _.get("location", ""))
        fp = open(MARKDOWN_SAVE_NAME, "w+")
        _text_head = """## 表 \n"""

        fp.write("\n" + _text_head + "\n")

        for node in nodes:
            node_other_att = {}
            # 未完成的节点为False
            if not node.get("status"):
                node_other_att.update(**{"fillcolor": "red", "style": "filled"})
            # 拼接 label
            location = node.get("location", "")
            schema = node.get("schema")
            meaning = node.get("meaning", [])

            schema_str = schema.simpleString() if schema else ""
            schema_name_type = [sub_str.split(":") for sub_str in schema_str.replace("struct<", "").replace(">", "").split(",")]
            schema_name_type = [v+[meaning[idx]] for (idx, v) in enumerate(schema_name_type)]

            _text_content = "\n".join(["""<tr>%s</tr>""" % "\n".join(["""<td align="center">%s</td>""" % c for c in row]) for row in zip(*schema_name_type)])
            _text_table = """<table><tbody><tr><td colspan="%s" align="center">%s</td></tr> %s </tbody></table>
            """ % (len(meaning) or 1, location, _text_content)
            _text_ship = "\n## %s\n" % node.get("name") + "\n%s\n" % (node.get("doc", "") or "") + "\n%s\n" % _text_table
            fp.write(_text_ship)

        fp.close()

    def bfs(self):
        source_set = set([link["source"] for link in self.Links])
        target_set = set([link["target"] for link in self.Links])
        root = source_set - target_set

        # 再加上不需要定时跑的表，且表已经存在了
        # not_need_to_run_target = set([link["target"] for link in self.Links if not link["need_to_run"]])
        # not_need_to_run_source = set([link["source"] for link in self.Links if not link["need_to_run"]])
        # not_need_to_run_and_exist_set = set([node["id"] for node in self.Nodes if node.get("status") and node["id"] in not_need_to_run_target])
        # root = root.union(not_need_to_run_and_exist_set)
        # root = root.union(not_need_to_run_source).union(not_need_to_run_source)

        step_limit = 10000
        pre_set = root

        for i in range(1, step_limit+1):
            # 查找能当前从存在的表中计算出的下一个表
            links_as_pre_source = [link for link in self.Links if link["source"] in pre_set and not link.get("step")]
            tmp_target_to_add_pre = set()
            for link_as_pre_source in links_as_pre_source:
                tmp_source_set = set([link["source"] for link in self.Links if link["cls_name"] == link_as_pre_source["cls_name"]])
                # 以target为终点的所有依赖的table都存在，则可以运行。
                if len(tmp_source_set - pre_set) == 0:
                    link_as_pre_source["step"] = i
                    tmp_target_to_add_pre.add(link_as_pre_source["target"])

            # 当前step判断完了之后才能加入到pre_set
            pre_set = pre_set.union(tmp_target_to_add_pre)

        to_left_set = target_set - pre_set
        to_left_link = [link for link in self.Links if link["target"] in to_left_set]

        to_run_links = [link for link in self.Links if link["need_to_run"]]
        to_run_links = sorted(to_run_links, key=lambda _: _.get("step"), reverse=False)
        to_run_links_dif = []
        to_run_cls_name_set = set()

        for a_t in to_run_links:
            if a_t["cls_name"] not in to_run_cls_name_set:
                to_run_links_dif.append(a_t)
                to_run_cls_name_set.add(a_t["cls_name"])

        return to_left_link, to_run_links, to_run_links_dif


class SparkTask(object):

    def __init__(self):
        self._depend_tables = []
        self._result_tables = []

    @property
    def depend_tables(self):
        return self._depend_tables

    @property
    def result_tables(self):
        return self._result_tables

    @staticmethod
    def get_file_path(): return os.path.abspath(__file__)

    @staticmethod
    def get_file_path_cls(): return os.path.abspath(__file__)

    def _get_cls_name(self):
        return str(self.__class__)

    def _get_cls(self):
        return self.__class__

    def add_depend_tables(self, *args):
        for tb in args:
            if isinstance(tb, TableInfo):
                self._depend_tables.append(tb)

    def add_result_tables(self, *args):
        for tb in args:
            if isinstance(tb, TableInfo):
                self._result_tables.append(tb)

    @abc.abstractmethod
    def set_table_info(self):
        raise NotImplementedError

    def get_spark_task(self):
        self.set_table_info()
        return {
            "cls": self._get_cls(),
            "cls_name": self._get_cls_name(),
            "path": self.get_file_path_cls(),
            "depend_tables": self._depend_tables,
            "result_tables": self._result_tables,
        }





