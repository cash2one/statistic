# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : custom_timely_scheduler.py

Authors: xulei12@baidu.com
Date   : 2016-7-5
Comment:
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from custom_index import test


class Command(BaseCommand):
    def assert_argv(self, *args):
        assert len(args) >= 3

    def handle(self, module_name, *args):
        u"""
        """
        test.main(module_name)

