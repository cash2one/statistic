# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : scheduler_for_time.py

Authors: xulei12@baidu.com
Date   : 2016.07.06
Comment:
"""
# 标准库
import datetime
# 第三方库

# 自有库
from lib import tools
import task_db


def main():
    tasks_id = task_db.CustomIndexTask.get_timely_tasks()
    for task_id in tasks_id:
        task = task_db.CustomIndexTask(task_id)
        if task.if_run_now():
            date = datetime.date.today()
            date = date.strftime("%Y%m%d")
            tools.run_main_cmd("custom_timely_import", [task.id, date])
