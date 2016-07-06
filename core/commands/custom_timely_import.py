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
from custom_index import import_timely_data


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

        if len(args) > 5:
            try:
                str_time = args[5]
                str_time = str_time.split("-")
                if str_time[1] < str_time[0]:
                    date_format = False
            except:
                date_format = False
            assert date_format

    def handle(self, task_id, date, ftp=None, time_span=None, *args):
        u"""params:
    task_id 导入任务id
    date  %Y%m%d格式日期
    ftp 下载数据的地址，可选
    time_span 导入的时间区间,格式 14:22-15:30 可选
        """
        if time_span:
            time_span = time_span.split("-")
        import_timely_data.main(task_id, date, ftp, time_span)

