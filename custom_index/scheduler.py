# coding=utf-8
import datetime

from lib import tools
import task_db
import data_db


def run_unroutine_task():
    tasks = task_db.CustomIndexTask.get_unroutine_tasks()
    today = datetime.date.today().strftime("%Y%m%d")
    for task in tasks:
        tools.run_main_cmd("custom_summary_import", [task, today], "log/custom_summary_import.log")


def run_routine_task(hour):
    tasks = task_db.CustomIndexTask.get_routine_tasks_by_hour(hour)
    today = datetime.date.today().strftime("%Y%m%d")
    for task in tasks:
        task = task_db.CustomIndexTask(task)
        date = datetime.date.today() - datetime.timedelta(days=task.time_delta)
        date = date.strftime("%Y%m%d")
        if task.task_type == "index":
            if task.if_run_today():
                tools.run_main_cmd("custom_original_import", [task.id, date], "log/custom_original_import.log")
                task.update_last_run_date(today)
            else:
                tools.run_main_cmd("custom_summary_import", [task.id, date], "log/custom_summary_import.log")
        elif task.task_type == "detail":
            tools.run_main_cmd("custom_detail_import", [task.id, date], "log/custom_detail_import.log")


def main():
    now = datetime.datetime.now()
    now_hour = now.hour
    tools.log('[INFO]start scheduler, hour %s' % now_hour)
    if now_hour == 0:
        run_unroutine_task()
    run_routine_task(now_hour)
