#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : tiyu.py

Authors: xulei12@baidu.com
Date   : 2017/1/3
Comment: 
"""
# 系统库

# 第三方库

# 自有库
try:
    import base_mapred
except:
    import midpage.base_mapred as base_mapred


class Mapred(base_mapred.BaseMapredLocal):
    pass

