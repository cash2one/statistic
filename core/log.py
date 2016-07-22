# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : log.py

Authors: yangxiaotong@baidu.com
Date   : 2016/5/31
Comment: 
"""
# 标准库
import os
import logging
import logging.handlers
# 第三方库

# 自有库


def init_log(log_path=None, level=logging.INFO, when="midnight", backup=7,
             format="%(levelname)s:[%(asctime)s][%(filename)s:%(lineno)d][%(thread)d] %(message)s",
             datefmt="%Y-%m-%d %H:%M:%S"):
    """
    init_log - initialize log module

    Args:
      log_path      - Log file path prefix.
                      Log data will go to two files: log_path.log and log_path.log.wf
                      Any non-exist parent directories will be created automatically
      level         - msg above the level will be displayed
                      DEBUG < INFO < WARNING < ERROR < CRITICAL
                      the default value is logging.INFO
      when          - how to split the log file by time interval
                      'S' : Seconds
                      'M' : Minutes
                      'H' : Hours
                      'D' : Days
                      'W' : Week day
                      default value: 'D'
      format        - format of the log
                      default format:
                      %(levelname)s: %(asctime)s: %(filename)s:%(lineno)d * %(thread)d %(message)s
                      INFO: 12-09 18:02:42: log.py:40 * 139814749787872 HELLO WORLD
      backup        - how many backup file to keep
                      default value: 7

    Raises:
        OSError: fail to create log directories
        IOError: fail to open log file
    """
    formatter = logging.Formatter(format, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)

    if log_path is not None:
        dir_name = os.path.dirname(log_path)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)

        handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log",
                                                            when=when,
                                                            backupCount=backup)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log.wf",
                                                            when=when,
                                                            backupCount=backup)
        handler.setLevel(logging.WARNING)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # added by xulei12@baidu.com 2016-07-21 订阅特性
        email_logger = logging.getLogger("email")
        handler = logging.handlers.TimedRotatingFileHandler(os.path.join(dir_name, "email.log"),
                                                            when=when,
                                                            backupCount=backup)
        handler.setFormatter(formatter)
        email_logger.addHandler(handler)
        # add ended 订阅特性

    handler = logging.StreamHandler()
    logger.addHandler(handler)
