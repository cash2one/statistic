#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : alarm_db.py

Authors: xulei12@baidu.com
Date   : 2016/10/10
Comment: 
"""
# 系统库

# 第三方库

# 自有库
import lib.mongo_db


class AlarmSetDb(lib.mongo_db.BaseMongoFontDb):
    """
    指标报警设置库
    """
    COLLECTION_NAME = "alarm_set"