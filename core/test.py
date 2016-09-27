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
import custom_index.task_db


def main(pack_name, module_name):
    module = importlib.import_module(pack_name+"."+module_name)
    module.test()
