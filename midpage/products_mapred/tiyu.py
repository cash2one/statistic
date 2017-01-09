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
import re
# 第三方库

# 自有库
try:
    import base_mapred_local
except:
    import midpage.base_mapred_local as base_mapred_local


class Mapred(base_mapred_local.BaseMapredLocal):
    """
    百度体育直播统计配置
    """
    ###################################
    # 指定日志来源。可以nanlin本地集群，可以为其他集群。其他集群会先执行distcp
    SOURCE = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310" \
             "/app/dt/minos/3/textlog/www/wise_tiyu_access/70025011/%s/1200/"
    ###################################
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^(?P<client_ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status>[0-9]+) ([0-9]+) "
                        r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" "
                        r"\"(?P<user_agent>.*)\" rt=(?P<cost_time>[0-9\.]+) [0-9]* "
                        r"([0-9\.]+) ([0-9\.]+) (.*) "
                        r"\".*\" ps appEngine - (?P<msec>[0-9\.]+)$")
    ###################################
    # 指标配置
    # example，具体类需要复写
    index_map = {
        # 每一项即为一个指标，KEY就是指标名，中文务必加 u
        u"pv": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "count",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "kgdc_file",
            "config": {},
            # 该指标对应的维度信息，在下面定义
            "group_name": "default_groups"
        }
    }
    ###################################
    # 用户画像配置
    ###################################
    # 用户路径分析配置
    ###################################
    groups = ["default_groups"]
    # groups中的2个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # key：表示在数据中，该维度对应的key名字。
    # 该配置 是对下面每个维度的一个汇总。可以存在多个维度汇总，名字自定义。
    default_groups = [{
        "key": "os",
        "attribute": "os_group",
    }, {
        "attribute": "client_group",
        "key": "client",
    }, {
        "attribute": "type_group",
        "key": "type",
    }]
    ###################################
    # 具体每个维度配置
    # 规定了该维度下，value的取值有哪些选项，名称自定义，上面的维度汇总配置引用
    # name：value取值项
    # query：匹配中该维度的查询条件
    os_group = [{
        "name": "ios",
        "query": {"os": "ios"},
    }, {
        "name": "android",
        "query": {"os": "android"},
    }, {
        "name": "other",
        "query": {"os": "other"},
    }]

    client_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "NA",
        "query": {"client": "NA"},
    }, {
        "name": "MB",
        "query": {"client": "MB"},
    }, {
        "name": "other",
        "query": {"os": "other"},
    }]

    type_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "detail",
        "query": {"url": "/detail"},
    }, {
        "name": "category",
        "query": {"url": "/category"},
    }, {
        "name": "news",
        "query": {"url": "/news"},
    }, {
        "name": "videodetail",
        "query": {"url": "/videodetail"},
    }]

