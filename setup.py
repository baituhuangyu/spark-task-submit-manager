#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup

setup(
    # name 本地引用目录名一直可以方便安装、卸载引用。setup 安装之后下划线会变为-，sc-risk-model-clean-utils
    name="sc_risk_model_clean_utils",
    version='1.0',
    packages=[],
    include_package_data=True,
    exclude_package_data={'': ['.gitignore']},
)

