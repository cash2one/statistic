# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : base.py

Authors: yangxiaotong@baidu.com
Date   : 2016/5/31
Comment: 
"""
# 标准库
import os
import logging
# 第三方库

# 自有库
from conf import conf
from lib import tools


def get_index(task_id, date, ftp):
    folder = os.path.join(conf.DATA_DIR, "custom_index/%s" % date)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, "%s.dat" % task_id)
    if "%s" in ftp:
        ftp = ftp % date
    tools.wget(ftp, path)
    return path


def check_line(json_line, must_keys):
    for key in must_keys:
        if key not in json_line:
            logging.error("[ERROR]miss key:%s=>%s" % (key, json_line))
            return False
    return True
