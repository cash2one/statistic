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
import os
import json
import logging
# 第三方库

# 自有库
from conf import conf
from lib import tools
import task_db
import data_db


def get_index(task_id, date, ftp):
    folder = os.path.join(conf.DATA_DIR, "custom_index/%s" % date)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, "%s.dat" % task_id)
    if "%s" in ftp:
        ftp = ftp % date
    tools.wget(ftp, path)
    return path


MUST_KEYS = ["@topic"]


def check_line(json_line, topic):
    for key in MUST_KEYS:
        if key not in json_line:
            logging.info("[ERROR]miss key:%s=>%s" % (key, json_line))
            return False
    if json_line["@topic"] != topic:
        logging.info("[ERROR]@topic:%s=>%s" % (topic, json_line))
        return False
    return True


def save_index(path, task, date):
    topic = task.topic
    system_key = {"@task": task.id, "@create": date}
    detail_data = data_db.DetailData(date)
    detail_data.remove(system_key)
    fp = open(path)
    for line in fp:
        try:
            line = line.rstrip("\r\n").decode("utf-8")
        except:
            logging.info("error line:%s" % line)
            continue
        try:
            json_line = json.loads(line)
        except:
            logging.info("[ERROR]json error:%s" % line)
        if check_line(json_line, topic):
            json_line.update(system_key)
            detail_data.insert(json_line)


def main(task_id, date, replace_ftp=None):
    logging.info("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    if task.task_type != "detail":
        logging.info('[ERROR]task type is %s' % task.task_type)
        exit(-1)
    # 获取数据
    if replace_ftp:
        ftp = replace_ftp
    else:
        ftp = task.path
    path = get_index(task_id, date, ftp)
    # 解析入库
    save_index(path, task, date)
    logging.info("[END]task id:%s, date:%s" % (task_id, date))
