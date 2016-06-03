# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : bdtj_dict_import.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from script import bdtj_dict_import


class Command(BaseCommand):
    u"""
    入库字词百度统计指标
    """
    def assert_argv(self, *args):
        assert len(args) == 3
        date_format = True
        try:
            time.strptime(args[2], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, date, *args):
        u"""params:
        date  %Y%m%d格式日期
        """    
        bdtj_dict_import.main(date)

