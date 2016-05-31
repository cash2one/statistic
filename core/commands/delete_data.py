# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : delete_data.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
# 第三方库

# 自有库
from core.management.base import BaseCommand
from core import delete


class Command(BaseCommand):
    u"""
    删除mongodb和data目录数据
    """

    def assert_argv(self, *args):
        assert len(args) >= 2

    def handle(self, date_limit=30, *args):
        u"""params:
    date_limit int格式，数据保留的最长天数，默认30天
        """
        delete.delete(date_limit)
