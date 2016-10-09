# coding=utf-8
import os

from midpage import base

source = 'qianxun'


class Product(base.CRMMidpageProduct):
    default_query = {
        'source': 'qianxun',
    }

    index_map = {  # example，具体类需要复写
        u'定制页pv': {
            'query': {
                'query.cat': 'dumi_qinzi_rss',
                'query.act': 'pv',
            },
            'type': 'count',
        },
        u'定制页uv': {
            'query': {
                'query.cat': 'dumi_qinzi_rss',
                'query.act': 'pv',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'定制页点击完成uv数': {
            'query': {
                'query.cat': 'dumi_qinzi_rss',
                'query.act':'a_click_rss'
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'定制页点击完成比': {
            'numerator': u'定制页点击完成uv数',
            'denominator': u'定制页uv',
            'type': 'percent',
        },
        u'列表页pv': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'pv',
            },
            'type': 'count',
        },
        u'列表页uv': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'pv',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'列表页tab点击数': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'a_click_tab',
            },
            'type': 'count',
        },
        u'列表页tab点击用户数': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'a_click_tab',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'列表页tab点击比': {
            'numerator': u'列表页tab点击用户数',
            'denominator': u'列表页uv',
            'type': 'percent',
        },
        u'列表页item点击数': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'a_click_activity_item',
            },
            'type': 'count',
        },
        u'列表页item点击用户数': {
            'query': {
                'query.cat': 'dumi_qinzi_list',
                'query.act':'a_click_activity_item',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'列表页item点击比': {
            'numerator': u'列表页item点击用户数',
            'denominator': u'列表页uv',
            'type': 'percent',
        },
        u'有点用户平均点击数': {
            'numerator': u'列表页item点击用户数',
            'denominator': u'列表页uv',
            'type': 'percent',
        }
    }

    index_group = [{
        'name': 'total',
        'query': {}
    }, {
        'name': 'ios',
        'query': {'os':'ios'}
    }, {
        'name': 'android',
        'query': {'os':'android'}
    }]

    file_group = [{
        'name': 'total',
        'query': {}
    }, {
        'name': 'NA',
        'query': {'client':'NA'}
    },{
        'name': 'MB',
        'query': {'client':'MB'}
    }]

    index_order = [
        u'定制页pv',
        u'定制页uv',
        u'定制页点击完成uv数',
        u'定制页点击完成比',
        u'列表页pv',
        u'列表页uv',
        u'列表页tab点击数',
        u'列表页tab点击用户数',
        u'列表页tab点击比',
        u'列表页item点击数',
        u'列表页item点击用户数',
        u'列表页item点击比',
        u'有点用户平均点击数',
    ]