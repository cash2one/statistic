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
        assert len(args) >= 4

    def handle(self, pack_name, module_name, *args):
        u"""
        测试代码命令。
        python main.py test {pack_name} {module_name}
        """
        test.main(pack_name, module_name)

