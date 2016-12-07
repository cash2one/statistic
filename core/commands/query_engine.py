# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : query_engine.py

Authors: xulei12@baidu.com
Date   : 2016-12-07
Comment:
通过提供的baiduid用户列表，通过query engine计算
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from query_engine import query_engine


class Command(BaseCommand):
    """
    汇总数据导入方式，命令行处理
    """
    def assert_argv(self, *args):
        """
        :param args:
        :return:
        """
        assert len(args) >= 4
        try:
            date = int(args[2])
        except:
            date = 0
        assert date

        try:
            baiduid = args[3]
        except:
            baiduid = False
        assert baiduid

        try:
            product = args[4]
        except:
            product = False
        assert product

    def handle(self, date, baiduid, product, *args):
        """
        根据生成的uid，来计算用户画像数据。
        params:
            date  %Y%m%d格式日期
            baiduid id文件，一行一个baiduid
            product 执行的query engine文件
        """
        query_engine.main(date, baiduid, product, *args)
