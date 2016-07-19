# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_original_data.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import json
import logging
import datetime
# 第三方库

# 自有库
import base
import task_db
import data_db
import reminder
from lib import tools
from lib import error

MUST_KEYS = ["@index", "@value"]


def save_index(path, task, date):
    sub_project = task.sub_project_id
    system_key = {"@task": task.id, "@create": date, "@subProject": sub_project}
    original_data = data_db.OriginalData()
    original_data.remove(system_key)
    fp = open(path)
    # added by xulei12@baidu.com 2016.07.18 订阅提醒
    indicators = []
    # add ended
    for line in fp:
        line = line.rstrip("\r\n").decode("utf-8")
        try:
            json_line = json.loads(line)
        except:
            logging.error("json error:%s" % line)
        if base.check_line(json_line, MUST_KEYS):
            json_line.update(system_key)
            try:
                original_data.insert(json_line)
                # added by xulei12@baidu.com 2016.07.18 订阅提醒
                if json_line["@index"] not in indicators:
                    indicators.append(json_line["@index"])
                # add ended
            except:
                tools.log("%s" % json_line)
                raise

    # added by xulei12@baidu.com 2016.07.18 订阅提醒
    remind = reminder.Reminder(task=task, date=date, mongo_db=original_data, indicators=indicators)
    remind.send_remind_email()
    # add ended


def get_summary_date(task_id, date):
    u"""
    更新指定的date，以及summary表中已有的，且比原始表中下一个日期小的日期
    达到的效果：
        1、对于例行指标更新最新指标情况下，只更新当天的summary
        2、对于非例行指标更新最新指标情况下，更新截止目前调度工具已更新的所有summary
        3、对于存在DATEA、DATEB两天指标情况下，更新DATEA，更新DATEA->DATEB之间的所有summary
    :param task_id:
    :param date:
    :return:
    """
    date_list = [date]
    original_data = data_db.OriginalData()
    last_date = original_data.find({"@create": {"$gt": date}, "@task": task_id}, {"@create": 1})
    last_date.sort([("@create", 1)])
    last_date = last_date[:1]
    last_date = list(last_date)
    if len(last_date) > 0:
        last_date = last_date[0]["@create"]
    else:
        # 若无更新的数据，则至明天
        today = datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        last_date = tomorrow.strftime("%Y%m%d")

    daily_summary_data = data_db.DailySummaryData()
    extend_date_list = daily_summary_data.find({"@date": {"$gt": date, "$lt": last_date}, "@task": task_id})
    extend_date_list = extend_date_list.distinct("@date")
    date_list.extend(extend_date_list)
    return date_list


def run_summary_cmd(date_list, task_id):
    for date in date_list:
        tools.run_main_cmd("custom_summary_import", [task_id, date])


def run(task_id, date, replace_ftp=None):
    logging.info("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    if task.task_type != "index":
        logging.fatal('task type is %s' % task.task_type)
        exit(-1)
    # 若启动时输入了ftp地址则覆盖任务默认地址
    if replace_ftp:
        ftp = replace_ftp
    else:
        ftp = task.path
    try:
        path = base.get_index(task_id, date, ftp)
    except error.DownloadError:
        if "%s" in ftp:
            ftp = ftp % date
        addr = '%s@baidu.com' % task.owner
        cc = 'kgdc-dev@baidu.com'
        title = u'%s 指标入库失败' % task.name
        text = u'%s 指标【任务id:%s】入库失败\n日期:%s\nftp地址:%s' %\
            (task.name, task.id, date, ftp)
        logging.fatal('wget error')
        tools.send_email(addr, title, text, cc=cc)
        exit(-1)
    else:
        # 解析入库
        save_index(path, task, date)
        # 获取待summary待更新日期
        date_list = get_summary_date(task_id, date)
        logging.info("summary date list:%s" % date_list)
        run_summary_cmd(date_list, task_id)
        logging.info("build output!")
        tools.run_main_cmd("custom_original_output", [task_id, date])
        task.update_last_run_date(date)
    logging.info("[END]task id:%s, date:%s" % (task_id, date))


def main(task_id, date, replace_ftp=None):
    try:
        run(task_id, date, replace_ftp)
    except SystemExit:
        pass
    except:
        logging.exception('')
        addr = 'kgdc-dev@baidu.com'
        title = u'指标入库任务【%s】失败' % task_id
        text = u'指标入库任务【%s】失败\n日期:%s' %\
               (task_id, date)
        tools.send_email(addr, title, text)
