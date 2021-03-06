# coding=utf-8
import os

from midpage import base

source = 'qianxun'

class Product(base.CRMMidpageProduct):
    default_query = {
        'source': 'qianxun',
        'query.cat': 'dumi_movie',
    }

    index_map = {
        u'pv': {
            'query': {
                'query.act': 'pv'
            },
            'type': 'count'
        },
        u'uv': {
            'query': {
                'query.act': 'pv'
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'播放点击': {
            'query': {
                'query.act': 'a_click_play'
            },
            'type': 'count',
        },
        u'预告片点击': {
            'query': {
                'query.act': 'a_click_trailer'
            },
            'type': 'count',
        },
        u'更多短评点击': {
            'query': {
                'query.act': 'a_click_more_comment'
            },
            'type': 'count',
        },
        u'更多影评点击': {
            'query': {
                'query.act': 'a_click_more_review'
            },
            'type': 'count',
        },
        u'切换播放源点击': {
            'query': {
                'query.act': 'b_click_change_playsource'
            },
            'type': 'count',
        },
        u'停留时长': {
            'query': {
                'query.act': 'stay_time',
                'query.duration': {'$lt': 1800},
            },
            'field': 'query.duration',
            'type': 'avg',
        },
        u'影评点击': {
            'query': {
                'query.act':'a_click_comment'
            },
            'type': 'count'
        },
        u'影人点击': {
            'query': {
                'query.act':'a_click_actor'
            },
            'type': 'count'
        },
        u'下拉来源点击': {
            'query': {
                'query.act':'a_click_more_source'
            },
            'type': 'count'
        },
        u'展开简介点击': {
            'query': {
                'query.act':'b_click_more_info'
            },
            'type': 'count'
        },
        u'剧照点击': {
            'query': {
                'query.act':'b_click_poster'
            },
            'type': 'count'
        },
    }


    index_group = [{
        'name': 'total',
        'query': {},
    }, {
        'name': 'ios',
        'query': {'os':'ios'},
    }, {
        'name': 'android',
        'query': {'os':'android'},
    }]

    file_group = [{
        'name': 'total',
        'query': {},
    }, {
        'name': 'NA',
        'query': {'client':'NA'},
    },{
        'name': 'MB',
        'query': {'client':'MB'},
    }]

    index_order = [
        u'pv',
        u'uv',
        u'播放点击',
        u'预告片点击',
        u'更多短评点击',
        u'更多影评点击',
        u'切换播放源点击',
        u'停留时长',
        u'影评点击',
        u'影人点击',
        u'下拉来源点击',
        u'展开简介点击',
        u'剧照点击',
    ]