#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : midpage_mapred.py

Authors: xulei12@baidu.com
Date   : 2017/1/4
Comment: 
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from midpage import base_mapred_local


class Command(BaseCommand):
    """
    该命令用于以mapred方式执行中间页指标计算任务。
    """

    def assert_argv(self, *args):
        """

        :param args:
        :return:
        """
        assert len(args) >= 2
        date_format = True
        try:
            time.strptime(args[2], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, date, products=None, *args):
        u"""
        该命令用于以mapred方式执行中间页指标计算任务。
        :param date: %Y%m%d格式日期
        :param products: （可选）不同产品，逗号分隔，可选，默认全部。产品名为midpage.products_mapred中的文件
        :param args:
        :return:
        """
        base_mapred_local.main(date, products)
