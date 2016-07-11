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
from custom_index import scheduler_for_timely


class Command(BaseCommand):
    def assert_argv(self, *args):
        pass

    def handle(self, *args):
        u"""
        """
        scheduler_for_timely.main()

