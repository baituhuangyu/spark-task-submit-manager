#!/usr/bin/env python
# encoding: utf-8

from pyspark.sql.types import StringType, StructField, StructType


class SchemaHelper(object):

    @staticmethod
    def list_to_struct_type_string(raw_list):
        if isinstance(raw_list, list):
            Exception("raw_list: %s" % type(raw_list))

        return StructType([StructField(field_name, StringType(), True) for field_name in raw_list])




