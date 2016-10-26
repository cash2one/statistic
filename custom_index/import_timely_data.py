# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_timely_data.py

Authors: xulei12@baidu.com
Date   : 2016-7-5
Comment: 时效性指标导入处理
"""
# 标准库
import json
import logging
import time
# 第三方库

# 自有库
import base
import task_db
import data_db
from lib import tools
from lib import error
import timely_data_process


MUST_KEYS = ["@index", "@value", "@time"]


def save_index(path, task, date, time_span=None):
    """
    如果不指定time_span， 则表示周期自动任务，不走清空流程
    :param path:
    :param task:
    :param date:
    :param time_span:
    :return:
    """
    sub_project = task.sub_project_id
    system_key = {"@task": task.id, "@subProject": sub_project}
    timely_data = data_db.TimelyData()
    sub_project_alarm_set = timely_data_process.get_alarm_set_by_sub_project(sub_project)
    # 清空数据流程，手动任务会进入该流程
    if time_span and len(time_span) >= 2:
        # 构造清空的query
        remove_query = system_key.copy()
        remove_query["@create"] = {"$gte": base.merge_date_time(date, time_span[0]),
                                   "$lte": base.merge_date_time(date, time_span[1])}
        logging.info("remove: %s" % json.dumps(remove_query, ensure_ascii=False))
        timely_data.remove(remove_query)
    fp = open(path)
    for line in fp:
        line = line.rstrip("\r\n").decode("utf-8")
        try:
            json_line = json.loads(line)
        except:
            logging.error("json error:%s" % line)
            continue
        if base.check_line(json_line, MUST_KEYS):
            # 指定了时间段，会对数据进行时间段内进行过滤
            if time_span and len(time_span) >= 2:
                if json_line["@time"] > time_span[1] or json_line["@time"] < time_span[0]:
                    continue
            json_line["@create"] = base.merge_date_time(date, json_line["@time"])
            json_line.update(system_key)
            update_query = system_key.copy()
            update_query["@create"] = json_line["@create"]
            update_query["@index"] = json_line["@index"]
            try:
                # logging.info("%s" % json.dumps(update_query, ensure_ascii=False))
                # logging.info("%s" % json.dumps(json_line, ensure_ascii=False))
                timely_data.update(update_query, json_line, True)
            except:
                logging.warning("write to timely_data error,\n%s" % json_line)
                raise

            # 更新最新指标值库
            timely_data_process.update_latest_data(json_line)
            # 报警配置
            timely_data_process.push_alarm_data(json_line, sub_project_alarm_set)


def run(task_id, date, time_span=None, replace_ftp=None):
    """
    如果不指定replace_ftp，则使用默认的地址配置。
    如果不指定time_span， 则表示周期自动任务，不走清空流程
    :param task_id:
    :param date:
    :param replace_ftp:
    :param time_span:  格式: 19:00-20:00
    :return:
    """
    logging.info("[BEGIN]task id:%s, date:%s, ftp:%s, time:%s" % (task_id, date, replace_ftp, time_span))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    if task.task_type != "timely":
        logging.fatal('task type is %s' % task.task_type)
        exit(-1)
    # 若启动时输入了ftp地址则覆盖任务默认地址
    if replace_ftp:
        ftp = replace_ftp
    else:
        ftp = task.path

    # 如果指定了时间跨度的处理
    if time_span and len(time_span) > 1:
        if time_span[1] < time_span[0]:
            logging.error("time_span error, end time(%s) is small than the start time(%s)" %
                          (time_span[1], time_span[0]))
            return
        update_time = time_span[1]
    else:
        update_time = time.strftime("%H:%M")

    # 获取文件
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
        # 时效性指标的日期格式，只是在存储入库的时候更改格式。
        date = time.strftime("%Y-%m-%d", time.strptime(date, "%Y%m%d"))
        # 解析入库
        save_index(path, task, date, time_span)
        # 更新最近运行时间
        task.update_last_run_time(date, update_time)
    logging.info("[END]task id:%s, date:%s, time:%s" % (task_id, date, update_time))


def main(task_id, date, time_span=None, replace_ftp=None):
    try:
        run(task_id, date, time_span, replace_ftp)
    except SystemExit:
        pass
    except:
        logging.exception('')
        addr = 'kgdc-dev@baidu.com'
        title = u'时效性指标入库任务【%s】失败' % task_id
        text = u'时效性指标入库任务【%s】失败\n日期:%s\n时间段:%s' %\
               (task_id, date, time_span)
        tools.send_email(addr, title, text)
