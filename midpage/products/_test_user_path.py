# coding=utf-8
"""
测试产品
"""
import os

from midpage import base

source = 'baidu_dictionary'


class Product(base.CRMMidpageProduct):
    """
    测试用
    """
    default_query = {
        'source': 'baidu_hanyu',
    }

    index_map = {
        "user_path": {
            "query": {},
            "type": "user_path",
            "loops": 5,
            # "no_group": True
        }
    }
    groups = [{
        "attribute": "index_group",
        "type": "index",
        "key": "@index",
    }]
    index_group = [{
        "name": "/zici/s",
        "query": {"url": "/zici/s"},
    }, {
        "name": "/shici/s",
        "query": {"url": "/shici/s"},
    }, {
        "name": "/shici/detail",
        "query": {"url": "/shici/detail"},
    }, {
        "name": "/s",
        "query": {"url": "/s"},
    }]
