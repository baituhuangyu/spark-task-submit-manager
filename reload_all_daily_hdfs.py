#!/usr/bin/env python
# encoding: utf-8

from controller.main_controller import Controller

# 重载所有hdfs
Controller().reload_all_daily_hdfs()
