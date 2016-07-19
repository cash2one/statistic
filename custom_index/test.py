# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : scheduler_for_time.py

Authors: xulei12@baidu.com
Date   : 2016.07.06
Comment:
"""
# 标准库
import datetime
import importlib
# 第三方库

# 自有库
from lib import tools
import task_db


def main(module_name):
    module_name = "custom_index." + module_name
    module = importlib.import_module(module_name)
    module.test()
