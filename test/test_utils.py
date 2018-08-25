#!/usr/bin/env python
# encoding: utf-8

import json


class TestUtils(object):

    @staticmethod
    def print_domain(obj):
        """
        领域对象打印
        """
        print json.dumps(obj, ensure_ascii=False, indent=4, default=lambda x: x.__dict__)

    @staticmethod
    def equal(var1, var2):
        """
        验证两个浮点数 对象是否大致相等
        """
        return var1 - var2 < 0.00001

    @staticmethod
    def assert_true(value, message=''):
        if not value:
            assert value, message + '期待值为 True, 实际为 False'

    @staticmethod
    def assert_false(value, message=''):
        if value:
            assert not value, message + '期待值为 False, 实际为 True'

    @staticmethod
    def assert_equal(expected, actual, message=''):
        if expected != actual:
            assert expected == actual, '{} 期待值:{} 实际值:{}'.format(message, expected, actual)

    @staticmethod
    def assert_in(i_str, must_in, message=''):
        if i_str not in must_in:
            assert i_str in must_in, '{} 输入值:{} 未找到包含在:{}'.format(message, i_str, must_in)

    @staticmethod
    def assert_not_in(i_str, must_in, message=''):
        if i_str in must_in:
            assert i_str not in must_in, '{} 输入值:{} 不能包含在:{}'.format(message, i_str, must_in)

    @staticmethod
    def assert_none(value, message=''):
        if value is not None:
            assert not value, '{}{}不为空'.format(value, message)

    @staticmethod
    def assert_not_none(value, message=''):
        if value is None:
            assert value, '{}{}为空'.format(value, message)

    @staticmethod
    def assert_not_equal(expected, actual, message=''):
        if expected == actual:
            assert expected != actual, '{} 期待不等于值:{} 实际值:{}'.format(message, expected, actual)

    @staticmethod
    def side_effect(reason_dict, default_value):
        """
        返回一个mock 方法
        :param reason_dict: 第一个输入值，输出值匹配字典表
        :param default_value: 未匹配的默认值
        :return: 用于设置 相关mock的inner_effect属性
        """

        def inner_effect(*arg):
            input_name = arg[0]
            if input_name in reason_dict.keys():
                return reason_dict[input_name]
            else:
                return default_value

        return inner_effect
