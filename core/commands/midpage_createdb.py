#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : midpage_createdb.py

Authors: xulei12@baidu.com
Date   : 2016/12/22
Comment: 
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from midpage import midpagedb


class Command(BaseCommand):
    """
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        """

        :param args:
        :return:
        """
        assert len(args) >= 3
        date_format = True
        try:
            time.strptime(args[2], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, date, *args):
        u"""

        :param date: %Y%m%d格式日期
        :param args: 可选
        :return:
        """
        midpagedb.createdb(date)
