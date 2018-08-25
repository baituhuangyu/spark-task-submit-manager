#!/usr/bin/env python3
# encoding: utf-8


class BaseTableInfo(object):
    table_info = None

    def __init__(self):
        if self.table_info is None:
            raise NotImplementedError

        self.table_info.__doc__ = self.__doc__
