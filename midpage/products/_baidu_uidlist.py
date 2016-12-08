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
        'source': 'baidu_dictionary',
    }

    index_map = {
        u'uidlist': {
            'query': {},
            'type': 'get_baiduid',
            'no_group': 'no_group'
        },

    }

