# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_data.py

Authors: xulei12@baidu.com
Date   : 2016-12-06
Comment:
汇总其他数据导入方式。目前包括：
user_portrait: 用户画像
user_path:     用户路径分析
"""
# 标准库
import time
# 第三方库

# 自有库
from core.management.base import BaseCommand
from custom_index import import_data


class Command(BaseCommand):
    def assert_argv(self, *args):
        assert len(args) >= 4
        try:
            task_id = int(args[2])
        except:
            task_id = 0
        assert task_id

        date_format = True
        try:
            time.strptime(args[3], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, task_id, date, ftp=None, *args):
        u"""
        汇总其他数据导入方式。目前包括的任务类型有：
            user_portrait 用户画像
            user_path     用户路径分析
        params:
            task_id 导入任务id
            date  %Y%m%d格式日期
            ftp 下载数据的地址，可选
        """
        import_data.main(task_id, date, ftp)
