# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : scheduler.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import logging
import datetime
# 第三方库

# 自有库
from lib import tools
import task_db


def run_unroutine_task():
    tasks = task_db.CustomIndexTask.get_unroutine_tasks()
    today = datetime.date.today().strftime("%Y%m%d")
    for task in tasks:
        tools.run_main_cmd("custom_summary_import", [task, today])


def run_routine_task(hour):
    tasks = task_db.CustomIndexTask.get_routine_tasks_by_hour(hour)
    today = datetime.date.today().strftime("%Y%m%d")
    for task in tasks:
        task = task_db.CustomIndexTask(task)
        date = datetime.date.today() - datetime.timedelta(days=task.time_delta)
        date = date.strftime("%Y%m%d")
        if task.task_type == "index":
            if task.if_run_today():
                tools.run_main_cmd("custom_original_import", [task.id, date])
                task.update_last_run_date(today)
            else:
                tools.run_main_cmd("custom_summary_import", [task.id, date])
        elif task.task_type == "detail":
            tools.run_main_cmd("custom_detail_import", [task.id, date])
        else:
            tools.run_main_cmd("other_import", [task.id, date])


def main():
    now = datetime.datetime.now()
    now_hour = now.hour
    logging.info('start scheduler, hour %s' % now_hour)
    if now_hour == 0:
        run_unroutine_task()
    run_routine_task(now_hour)
