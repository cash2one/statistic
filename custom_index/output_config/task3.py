# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : task3.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库

# 第三方库

# 自有库

index_map = {
    "client": [{
        "key": "shoubai",
        "name": u"手机百度",
        "filename": "MB",
    }, {
        "key": "app",
        "name": u"度秘APP",
        "filename": "NA",
    }],
    "system": [{
        "key": "all",
        "name": u"总体",
    }, {
        "key": "ios",
        "name": u"苹果",
    }, {
        "key": "android",
        "name": u"安卓",
    }],
    "@index": [{
        "key": "pv",
        "name": "pv",
    }, {
        "key": "uv",
        "name": "uv",
    }, {
        "key": "sess_num",
        "name": u"session数量",
    }, {
        "key": "single_click",
        "name": u"单实体卡片点击数",
    }, {
        "key": "multi_click",
        "name": u"多实体卡片点击数",
    }, {
        "key": "single_click_rate",
        "name": u"单实体卡片点击率",
    }, {
        "key": "multi_click_rate",
        "name": u"多实体卡片点击率",
    }, {
        "key": "single_dsp",
        "name": u"单实体卡片展现数",
    }, {
        "key": "multi_dsp",
        "name": u"多实体卡片展现数",
    }, {
        "key": "avg_interact_round",
        "name": u"平均交互轮数",
    }]
}

col_list = [
    "client",
    "system",
    "@index",
]
