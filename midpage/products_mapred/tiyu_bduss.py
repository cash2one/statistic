#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : tiyu_bduss.py

Authors: xulei12@baidu.com
Date   : 2017/2/9
Comment: 
"""
# 系统库

# 第三方库

# 自有库
import tiyu


class Mapred(tiyu.Mapred):
    """
    为了计算LV，只能再开一个mapred任务。按照BDUSS来分发key。其他配置继承自tiyu
    """
    # map任务中分发key的配置
    SORT_KEY = "BDUSS"
    DROP_NO_KEY = True
    # 指标配置
    index_map = {
        # 每一项即为一个指标，KEY就是指标名，中文务必加 u
        u"lv": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.act": "pv"
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "distinct_count",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "mapper": "BDUSS",
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "pv_groups"
        }
    }
