# coding=utf-8
import os

from midpage import base


class Product(base.CRMMidpageProduct):
    default_query = {
        'query.cat': 'dumi_movie_test_sam_wise_kg_normal_score'
    }

    index_map = { #example，具体类需要复写
        u'对话导流点击': {
            'query': {
                'query.act':'a_click_dialog_enter'
            },
            'type': 'count'
        },
        u'浮层导流点击': {
            'query': {
                'query.act':'a_click_hover_enter'
            },
            'type': 'count'
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
        u'对话导流点击',
        u'浮层导流点击',
        u'影评点击',
        u'影人点击',
        u'下拉来源点击',
        u'展开简介点击',
        u'剧照点击',
    ]