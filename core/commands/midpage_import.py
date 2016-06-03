# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : midpage_import.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import re
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from script import midpage_import


class Command(BaseCommand):
    u"""
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        assert len(args) == 4
        assert re.compile('\d+').match(args[2])
        date_format = True
        try:
            time.strptime(args[3], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, product_id, date, *args):
        u"""该命令用于将RD统计好的数据存入mysql，与新版中间页统计无关
params:
    product_id  数据库中的产品id
    date  %Y%m%d格式日期
        """    
        midpage_import.main(product_id, date)

