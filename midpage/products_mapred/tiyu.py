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
             "/app/dt/minos/3/textlog/www/wise_tiyu_access/70025011/%s"
    # SOURCE = "/app/ps/spider/wdmqa/kgdc/20170111/tiyu/input/2000"
    ###################################
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^(?P<client_ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status>[0-3][0-9]+) ([0-9]+) "
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
                "query.act": "pv"
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "count",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "pv_groups"
        },
        u"uv": {
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
                "mapper": "BAIDUID",
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "pv_groups"
        },
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
                    "target": ["/live", "/category", "/detail", "/player", "/news", "/videodetail"],
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
    groups = ["pv_groups", "user_path_groups"]
    # groups中的2个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # key：表示在数据中，该维度对应的key名字。
    # 该配置 是对下面每个维度的一个汇总。可以存在多个维度汇总，名字自定义。
    pv_groups = [{
        "key": "page",
        "attribute": "page_group"
    }, {
        "key": "tab",
        "attribute": "tab_group"
    }]
    user_path_groups = [{
        "key": "url",
    }, {
        "key": "referr",
    }]
    ###################################
    # 具体每个维度配置
    # 规定了该维度下，value的取值有哪些选项，名称自定义，上面的维度汇总配置引用
    # name：value取值项
    # query：匹配中该维度的查询条件
    page_group = [{
        "name": "total",
        "query": {}
    }, {
        "name": "live",
        "query": {"query.page": "live"}
    }, {
        "name": "category",
        "query": {"query.page": "category"}
    }, {
        "name": "detail",
        "query": {"query.page": "detail"}
    }, {
        "name": "player",
        "query": {"query.page": "player"}
    }, {
        "name": "news",
        "query": {"query.page": "news"}
    }, {
        "name": "videodetail",
        "query": {"query.page": "videodetail"}
    }, {
        "name": "personal",
        "query": {"query.page": "personal"}
    }, {
        "name": "christmaswars",
        "query": {"query.page": "christmaswars"}
    }, {
        "name": "address",
        "query": {"query.page": "address"}
    }, {
        "name": "cctv",
        "query": {"query.page": "cctv"}
    }]

    tab_group = [{
        "name": "total",
        "query": {},
    }, {
         "name": "jinrituijian",
         "query": {"query.tab": u"今日推荐"}
    }, {
         "name": "zhibo",
         "query": {"query.tab": u"直播"}
    }, {
         "name": "nba",
         "query": {"query.tab": u"NBA"}
    }, {
         "name": "cba",
         "query": {"query.tab": u"CBA"}
    }, {
         "name": "yingchao",
         "query": {"query.tab": u"英超"}
    }, {
         "name": "xijia",
         "query": {"query.tab": u"西甲"}
    }, {
         "name": "dejia",
         "query": {"query.tab": u"德甲"}
    }, {
         "name": "wwe",
         "query": {"query.tab": u"WWE"}
    }, {
         "name": "yijia",
         "query": {"query.tab": u"意甲"}
    }, {
         "name": "yaguan",
         "query": {"query.tab": u"亚冠"}
    }, {
         "name": "fajia",
         "query": {"query.tab": u"法甲"}
    }, {
         "name": "ouguan",
         "query": {"query.tab": u"欧冠"}
    }, {
         "name": "oulianbei",
         "query": {"query.tab": u"欧联杯"}
    }, {
         "name": "nfl",
         "query": {"query.tab": u"NFL"}
    }, {
         "name": "shiyusai",
         "query": {"query.tab": u"世预赛"}
    }, {
         "name": "wangqiu",
         "query": {"query.tab": u"网球"}
    }, {
         "name": "gaoerfu",
         "query": {"query.tab": u"高尔夫"}
    }, {
         "name": "yumaoqiu",
         "query": {"query.tab": u"羽毛球"}
    }, {
         "name": "zhongchao",
         "query": {"query.tab": u"中超"}
    }, {
         "name": "shijubei",
         "query": {"query.tab": u"世俱杯"}
    }]
