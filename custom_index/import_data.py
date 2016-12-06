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

# key为支持的任务类型
MUST_KEYS = {
    "user_portrait": ["@index", "@value"],
    "user_path": ["source", "target", "value"]
}

# mongodb索引
DB_MAP = {
    "user_portrait": data_db.UserPortrait(),
    "user_path": data_db.UserPath()
}


def save_index(path, task, date):
    sub_project = task.sub_project_id
    system_key = {"@task": task.id, "@create": date, "@subProject": sub_project}
    db = DB_MAP[task.task_type]
    db.remove(system_key)
    fp = open(path)

    # add ended
    for line in fp:
        line = line.rstrip("\r\n").decode("utf-8")
        try:
            json_line = json.loads(line)
        except:
            logging.error("json error:%s" % line)
        if base.check_line(json_line, MUST_KEYS[task.task_type]):
            json_line.update(system_key)
            try:
                db.insert(json_line)
            except:
                logging.error("insert mongo error: %s" % json_line)
                raise


def run(task_id, date, replace_ftp=None):
    logging.info("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    # 任务类型判断。
    if task.task_type not in MUST_KEYS.keys():
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
