# coding=utf-8
"""
百度汉语产品
"""
import os

from midpage import base

source = 'baidu_hanyu'

class Product(base.CRMMidpageProduct):
    """
    获得百度汉语的百度ID list
    """
    default_query = {
        'source': 'baidu_hanyu',
    }

    index_map = {
        u'uidlist': {
            'query': {},
            'type': 'get_baiduid',
        }

    }
