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


def main(module_name, function_name, *args):
    """

    :param pack_name:
    :param module_name:
    :param function_name:
    :return:
    """
    module = importlib.import_module(module_name)
    if not function_name:
        ret = module.test()
    else:
        # print module
        func = vars(module)[function_name]
        ret = func(*args)
    print "the return value is :\n%s" % ret
