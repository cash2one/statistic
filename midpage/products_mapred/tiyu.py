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
    # SOURCE = "/app/ps/spider/wdmqa/kgdc/20170208/tiyu/input"
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
        u"click": {
            # query为计算指标需要用的参数或者字段
            "query": {
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
            "group_name": "click_groups"
        },
        u"ended": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.tab": u"已结束"
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
            "group_name": "ended_groups"
        },
        u"not-ended": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.tab": u"未结束"
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
            "group_name": "ended_groups"
        },
        u"rank0": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.rank": "0"
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
            "group_name": "rank_groups"
        },
        u"rank1": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.rank": "1"
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
            "group_name": "rank_groups"
        },
        u"rank2": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.rank": "2"
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
            "group_name": "rank_groups"
        },
        u"rank3": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.rank": "3"
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
            "group_name": "rank_groups"
        },
        u"rank4": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.extend.rank": "4"
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
            "group_name": "rank_groups"
        },
        u"rank0_ratio": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "",
            "reducer": "",
            # local是mapred任务完成后，在本地执行的操作
            "gather": "among_indexes",
            "config": {
                "gather": {
                    # 后验计算需要的指标
                    "index": ["rank0", "pv"],
                    # 计算方法
                    "method": "divide",
                    # 通过第一个指标查找第二个指标时候，需要剔除的维度
                    "reject_dimension": ["type"],
                    # 最终需要输出的位置
                    "file": "%s.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"rank1_ratio": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "",
            "reducer": "",
            # local是mapred任务完成后，在本地执行的操作
            "gather": "among_indexes",
            "config": {
                "gather": {
                    # 后验计算需要的指标
                    "index": ["rank1", "pv"],
                    # 计算方法
                    "method": "divide",
                    # 通过第一个指标查找第二个指标时候，需要剔除的维度
                    "reject_dimension": ["type"],
                    # 最终需要输出的位置
                    "file": "%s.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"rank2_ratio": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "",
            "reducer": "",
            # local是mapred任务完成后，在本地执行的操作
            "gather": "among_indexes",
            "config": {
                "gather": {
                    # 后验计算需要的指标
                    "index": ["rank2", "pv"],
                    # 计算方法
                    "method": "divide",
                    # 通过第一个指标查找第二个指标时候，需要剔除的维度
                    "reject_dimension": ["type"],
                    # 最终需要输出的位置
                    "file": "%s.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"rank3_ratio": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "",
            "reducer": "",
            # local是mapred任务完成后，在本地执行的操作
            "gather": "among_indexes",
            "config": {
                "gather": {
                    # 后验计算需要的指标
                    "index": ["rank3", "pv"],
                    # 计算方法
                    "method": "divide",
                    # 通过第一个指标查找第二个指标时候，需要剔除的维度
                    "reject_dimension": ["type"],
                    # 最终需要输出的位置
                    "file": "%s.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"rank4_ratio": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "",
            "reducer": "",
            # local是mapred任务完成后，在本地执行的操作
            "gather": "among_indexes",
            "config": {
                "gather": {
                    # 后验计算需要的指标
                    "index": ["rank4", "pv"],
                    # 计算方法
                    "method": "divide",
                    # 通过第一个指标查找第二个指标时候，需要剔除的维度
                    "reject_dimension": ["type"],
                    # 最终需要输出的位置
                    "file": "%s.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
    }
    ###################################
    # 用户画像配置
    ###################################
    # 用户路径分析配置
    ###################################
    groups = ["pv_groups", "user_path_groups", "click_groups", "ended_groups", "rank_groups"]
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
    click_groups = [{
        "key": "page",
        "attribute": "extend_page_group"
    }, {
        "key": "tab",
        "attribute": "extend_tab_group"
    }, {
        "key": "type",
        "attribute": "extend_type_group"
    }]
    ended_groups = [{
        "key": "page",
        "attribute": "extend_page_ended_group"
    }, {
        "key": "tab",
        "attribute": "extend_tab_ended_group"
    }, {
        "key": "type",
        "attribute": "extend_type_ended_group"
    }]
    rank_groups = [{
        "key": "page",
        "attribute": "extend_page_group"
    }, {
        "key": "tab",
        "attribute": "extend_tab_rank_group"
    }, {
        "key": "type",
        "attribute": "extend_type_rank_group"
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
         "query": {"query.tab": "NBA"}
    }, {
         "name": "cba",
         "query": {"query.tab": "CBA"}
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
         "query": {"query.tab": "WWE"}
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
         "query": {"query.tab": "NFL"}
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
    }, {
        "name": "schedule",
        "query": {"query.tab": "schedule"}
    }, {
        "name": "video",
        "query": {"query.tab": "video"}
    }, {
        "name": "news",
        "query": {"query.tab": "news"}
    }, {
        "name": "battle",
        "query": {"query.tab": "battle"}
    }, {
        "name": "sort",
        "query": {"query.tab": "sort"}
    }, {
        "name": "qianzhan",
        "query": {"query.tab": "qianzhan"}
    }, {
        "name": "xinwen",
        "query": {"query.tab": "xinwen"}
    }, {
        "name": "zhenrong",
        "query": {"query.tab": "zhenrong"}
    }, {
        "name": "nba",
        "query": {"query.tab": "nba"}
    }, {
        "name": "saikuang",
        "query": {"query.tab": "saikuang"}
    }, {
        "name": "graphicLive",
        "query": {"query.tab": "graphicLive"}
    }, {
        "name": "jijin",
        "query": {"query.tab": "jijin"}
    }, {
        "name": "news",
        "query": {"query.tab": "news"}
    }, {
        "name": "data",
        "query": {"query.tab": "data"}
    }, {
        "name": "info",
        "query": {"query.tab": "info"}
    }, {
        "name": "video",
        "query": {"query.tab": "video"}
    }]

    extend_page_group = [{
        "name": "total",
        "query": {}
    }, {
        "name": "live",
        "query": {"query.extend.page": "live"}
    }, {
        "name": "category",
        "query": {"query.extend.page": "category"}
    }, {
        "name": "detail",
        "query": {"query.extend.page": "detail"}
    }, {
        "name": "news",
        "query": {"query.extend.page": "news"}
    }, {
        "name": "videodetail",
        "query": {"query.extend.page": "videodetail"}
    }]

    extend_tab_group = [{
        "name": "total",
        "query": {},
    }, {
         "name": "jinrituijian",
         "query": {"query.extend.tab": u"今日推荐"}
    }, {
        "name": "xinwen",
        "query": {"query.extend.tab": u"新闻"}
    }, {
        "name": "saicheng",
        "query": {"query.extend.tab": u"赛程"}
    }, {
        "name": "shipin",
        "query": {"query.extend.tab": u"视频"}
    }, {
        "name": "jifenbang",
        "query": {"query.extend.tab": u"积分榜"}
    }, {
        "name": "zhenrong",
        "query": {"query.extend.tab": u"阵容"}
    }, {
        "name": "jijin",
        "query": {"query.extend.tab": u"集锦"}
    }, {
        "name": "qianzhan",
        "query": {"query.extend.tab": u"前瞻"}
    }, {
        "name": "tongji",
        "query": {"query.extend.tab": u"统计"}
    }, {
        "name": "saikuang",
        "query": {"query.extend.tab": u"赛况"}
    }]

    extend_type_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "tab",
        "query": {"query.extend.type": "tab"}
    }, {
        "name": "show-all",
        "query": {"query.extend.type": "show-all"}
    }, {
        "name": "record-schedule",
        "query": {"query.extend.type": "record-schedule"}
    }, {
        "name": "record-news",
        "query": {"query.extend.type": "record-news"}
    }, {
        "name": "record-video",
        "query": {"query.extend.type": "record-video"}
    }, {
        "name": "record-date",
        "query": {"query.extend.type": "record-date"}
    }, {
        "name": "share",
        "query": {"query.extend.type": "share"}
    }, {
        "name": "back",
        "query": {"query.extend.type": "back"}
    }, {
        "name": "refresh",
        "query": {"query.extend.type": "refresh"}
    }, {
        "name": "go2top",
        "query": {"query.extend.type": "go2top"}
    }, {
        "name": "go2home",
        "query": {"query.extend.type": "go2home"}
    }, {
        "name": "show-more",
        "query": {"query.extend.type": "show-more"}
    }]

    extend_page_ended_group = [{
        "name": "live",
        "query": {"query.extend.page": "live"}
    }]
    extend_tab_ended_group = [{
        "name": "total",
        "query": {}
    }]
    extend_type_ended_group = [{
        "name": "button",
        "query": {"query.extend.type": "tab"}
    }]

    extend_tab_rank_group = [{
        "name": "total",
        "query": {},
    }, {
         "name": "jinrituijian",
         "query": {"query.extend.tab": u"今日推荐"}
    }]

    extend_type_rank_group = [{
        "name": "total",
        "query": {},
    }, {
         "name": "slide",
         "query": {"query.extend.type": "slide"}
    }, {
        "name": "schedule-title",
        "query": {"query.extend.type": "schedule-title"}
    }, {
        "name": "schedule-content",
        "query": {"query.extend.type": "schedule-content"}
    }, {
        "name": "feed-video",
        "query": {"query.extend.type": "feed-video"}
    }, {
        "name": "feed-news",
        "query": {"query.extend.type": "feed-news"}
    }, {
        "name": "recommend",
        "query": {"query.extend.type": "recommend"}
    }, {
        "name": "player",
        "query": {"query.extend.type": "player"}
    }, {
        "name": "sort",
        "query": {"query.extend.type": "sort"}
    }]
