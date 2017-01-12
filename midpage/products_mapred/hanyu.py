#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : hanyu.py

Authors: xulei12@baidu.com
Date   : 2017/1/10
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
    百度汉语统计配置
    """
    ###################################
    # 指定日志来源。可以nanlin本地集群，可以为其他集群。其他集群会先执行distcp
    SOURCE = "/app/ps/spider/wdmqa/kgdc/%s/hanyu/input"
    ###################################
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^([0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status_code>[0-9]{3}) (\d+) "
                        r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" \"(?P<user_agent>.*)\" "
                        r"(?P<cost_time>[0-9\.]+) ([0-9]+) ([0-9\.]+) ([0-9\.]+) (.+) (.*) "
                        r"\"(.*)\" (\w*) (\w*) (\d+) (?P<timestamp>[0-9\.]+)$")
    ###################################
    # 指标配置
    # example，具体类需要复写
    index_map = {
        # 每一项即为一个指标，KEY就是指标名，中文务必加 u
        u"uid_list": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "distinct",
            "reducer": "distinct",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "mapper": "BAIDUID",
                "reducer": "",
                "local": "%s_uid.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"user_path": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "user_path_mapper",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "user_path_local",
            # 以上三个过程都是针对mapred操作特点，对每一行进行处理。
            # gather是搜集所有信息后，对汇总信息进行处理。诸如指标之间的运算，汇总后处理等。
            "gather": "user_path_gather",
            "config": {
                "gather": {
                    "target": ["/s", "/zici/s", "/shici/s", "/shici/detail"],
                    "file": "%s_user_path.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "user_path_groups"
        },
    }
    ###################################
    # 用户画像配置
    ###################################
    # 用户路径分析配置
    ###################################
    groups = ["user_path_groups"]
    # groups中的2个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # key：表示在数据中，该维度对应的key名字。
    # 该配置 是对下面每个维度的一个汇总。可以存在多个维度汇总，名字自定义。

    ###################################
    # 具体每个维度配置
    # 规定了该维度下，value的取值有哪些选项，名称自定义，上面的维度汇总配置引用
    # name：value取值项
    # query：匹配中该维度的查询条件
    user_path_groups = [{
        "key": "url",
    }, {
        "key": "referr",
    }]