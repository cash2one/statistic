# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : midpage_analysis.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from midpage import analysis_mapred


class Command(BaseCommand):
    """
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

    def handle(self, date, sources=None, *args):
        """
        用mapred方式分析日志
        params:
            date  %Y%m%d格式日期
            sources 分来源进行统计，逗号分隔，可选，默认全部
        """
        analysis_mapred.main(date, sources)

