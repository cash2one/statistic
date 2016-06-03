# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : custom_import_scheduler.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库

# 第三方库

# 自有库
from core.management.base import BaseCommand
from custom_index import scheduler


class Command(BaseCommand):

    def assert_argv(self, *args):
        pass

    def handle(self, *args):
        u""""""
        scheduler.main()
