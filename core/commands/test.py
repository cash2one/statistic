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
# 第三方库

# 自有库
from core import test
from core.management.base import BaseCommand


class Command(BaseCommand):
    def assert_argv(self, *args):
        assert len(args) >= 3

    def handle(self, module_name, function_name=None, *args):
        u"""
        测试代码命令。
        python main.py test {module_name} [{function_name} {参数}]
        如果不带function_name, 则自动执行module里的 test(), 例如:
          python main.py test lib.tools
          python main.py test lib.tools clear_files /home/work/temp 30
        """
        test.main(module_name, function_name, *args)
