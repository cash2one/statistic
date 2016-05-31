# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : createdb.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库

# 第三方库

# 自有库
from core.management.base import BaseCommand
from core import createdb


class Command(BaseCommand):
    u"""
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        assert len(args) == 3
        assert args[2] in ["this", "last", "next", "init"]  

    def handle(self, mode="next", *args):
        u"""mode:
    this  创建本月的数据库
    last  创建上月的数据库
    next  默认值，创建下月的数据库
    init  初始化，创建所有表和本月的表
        """    
        createdb.create(mode)

