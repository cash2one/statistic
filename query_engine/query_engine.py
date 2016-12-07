#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : queryengine.py

Authors: xulei12@baidu.com
Date   : 2016/12/7
Comment: 
"""
# 系统库
import os

# 第三方库

# 自有库
import conf.conf as conf


def main(date, baiduid, product, *args):
    """

    :param date:
    :param baiduid:
    :param product:
    :param args:
    :return:
    """
    qe = os.path.join(conf.QUERY_ENGINE_PATH, "bin/queryengine")
    print qe
