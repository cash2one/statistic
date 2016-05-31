# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_detail_data.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import json
import logging
# 第三方库

# 自有库
import base
import task_db
import data_db

MUST_KEYS = ["@topic"]


def save_index(path, task, date):
    sub_project = task.sub_project_id
    system_key = {"@task": task.id, "@create": date, "@subProject": sub_project}
    detail_data = data_db.DetailData(date)
    detail_data.remove(system_key)
    fp = open(path)
    for line in fp:
        try:
            line = line.rstrip("\r\n").decode("utf-8")
        except:
            logging.error("error line:%s" % line)
            continue
        try:
            json_line = json.loads(line)
        except:
            logging.error("[ERROR]json error:%s" % line)
        if base.check_line(json_line, MUST_KEYS):
            json_line.update(system_key)
            detail_data.insert(json_line)


def main(task_id, date, replace_ftp=None):
    logging.info("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    if task.task_type != "detail":
        logging.fatal('task type is %s' % task.task_type)
        exit(-1)
    # 获取数据
    if replace_ftp:
        ftp = replace_ftp
    else:
        ftp = task.path
    path = base.get_index(task_id, date, ftp)
    # 解析入库
    save_index(path, task, date)
    logging.info("[END]task id:%s, date:%s" % (task_id, date))
