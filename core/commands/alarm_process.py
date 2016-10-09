#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : alarm_process.py

Authors: xulei12@baidu.com
Date   : 2016/9/23
Comment: 
"""
# 系统库

# 第三方库


# 自有库
from core.management.base import BaseCommand
import alarm.alarm_process


class Command(BaseCommand):
    def assert_argv(self, *args):
        pass

    def handle(self, *args):
        u"""
        处理redis中报警任务进程
        """
        alarm.alarm_process.main()
