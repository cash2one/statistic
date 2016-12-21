# coding=utf-8
"""
百度汉语产品
"""
import os

from midpage import base

source = 'hanyu'


class Product(base.CRMMidpageProduct):
    """
    获得百度汉语的百度ID list
    """
    default_query = {
        'source': 'hanyu',
    }

    index_map = {
        'uidlist': {
            'query': {},
            'type': 'get_baiduid',
            "no_group": True
        },
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
