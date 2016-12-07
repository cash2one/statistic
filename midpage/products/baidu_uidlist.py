# coding=utf-8
import os

from midpage import base

source = 'baidu_dictionary'

class Product(base.CRMMidpageProduct):
    default_query = {
        'source': 'baidu_dictionary',
    }

    index_map = {
        u'uidlist': {
            'query': {},
            'type': 'get_baiduid',
        },

    }