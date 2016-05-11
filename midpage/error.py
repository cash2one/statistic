#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : error.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-20
Comment:
"""


class ParseLineError(Exception):
    u"""错误的行，会被略过
    """
    pass
