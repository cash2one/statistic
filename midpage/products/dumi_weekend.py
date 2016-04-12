# coding=utf-8
import os

from midpage import base

source = 'qianxun'



class Product(base.CRMMidpageProduct):
    defaul_query = {
        'source': 'qianxun',
    }

    index_map = { #example，具体类需要复写
        u'列表页card展现总数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act': {
                    '$in': ['a_click_tab', 'stay_time', 'a_click_activity_item'],
                },
            },
            'field': 'query.extend.card_num',
            'type': 'sum'
        },
        u'列表页tab展现总数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'a_click_tab'
            },
            'field': 'query.extend.tab_num',
            'type': 'sum'
        },
        u'列表页卡片点击总次数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'a_click_activity_item',
            },
            'type': 'count',
        },
        u'列表页tab点击总次数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'a_click_tab',
            },
            'type': 'count',
        },
        u'列表页tab点展比': {
            'numerator': u'列表页tab点击总次数',
            'denominator': u'列表页tab展现总数',
            'type': 'percent',
        },
        u'列表页卡片点展比': {
            'numerator': u'列表页卡片点击总次数',
            'denominator': u'列表页card展现总数',
            'type': 'percent',
        },
        u'列表页收藏总数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'b_activity_active_collect',
            },
            'type': 'count',
        },
        u'列表页取消收藏总数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'b_activity_cancel_collect',
            },
            'type': 'count',
        },
        u'列表页收藏点击率': {
            'numerator': u'列表页收藏总数',
            'denominator': u'列表页card展现总数',
            'type': 'percent',
        },
        u'列表页取消收藏点击率': {
            'numerator': u'列表页取消收藏总数',
            'denominator': u'列表页card展现总数',
            'type': 'percent',
        },
        u'列表页平均停留时间': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'stay_time'
            },
            'field': 'query.duration',
            'type': 'avg'
        },
        u'列表页PV': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act': {
                    '$in': ['pv', 'a_click_tab'],
                },
            },
            'type': 'count',
        },
        u'列表页UV': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act': {
                    '$in': ['pv', 'a_click_tab'],
                },
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'列表页卡片点击率': {
            'numerator': u'列表页卡片点击总次数',
            'denominator': u'列表页PV',
            'type': 'percent',
        },
        u'收藏页PV': {
            'query': {
                'query.cat': 'dumi_weekend_collect',
                'query.act': 'pv',
            },
            'type': 'count',
        },
        u'收藏页UV': {
            'query': {
                'query.cat': 'dumi_weekend_collect',
                'query.act': 'pv',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'收藏页取消收藏总数': {
            'query': {
                'query.cat': 'dumi_weekend_collect',
                'query.act':'b_activity_cancel_collect',
            },
            'type': 'count',
        },
        u'收藏页取消收藏点击率': {
            'numerator': u'收藏页取消收藏总数',
            'denominator': u'收藏页PV',
            'type': 'percent',
        },
        u'收藏页卡片点击总次数': {
            'query': {
                'query.cat': 'dumi_weekend_collect',
                'query.act':'a_click_activity_item',
            },
            'type': 'count',
        },
        u'收藏页卡片点击率': {
            'numerator': u'收藏页卡片点击总次数',
            'denominator': u'收藏页PV',
            'type': 'percent',
        },
        u'收藏页平均停留时间': {
            'query': {
                'query.cat': 'dumi_weekend_collect',
                'query.act':'stay_time'
            },
            'field': 'query.duration',
            'type': 'avg',
        },
        u'整体PV': {
            'index': [u'列表页PV', u'收藏页PV'],
            'type': 'add',
        },
        u'整体UV': {
            'query': {
                'query.cat': {
                    '$in': ['dumi_weekend_collect', 'dumi_weekend'],
                },
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'列表页进入次数': {
            'query': {
                'query.cat': 'dumi_weekend',
                'query.act':'pv',
            },
            'type': 'count',
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
        u'列表页tab点展比',
        u'列表页卡片点展比',
        u'列表页收藏点击率',
        u'列表页平均停留时间',
        u'列表页进入次数',
        u'列表页PV',
        u'列表页UV',
        u'列表页卡片点击率',
        u'收藏页取消收藏点击率',
        u'收藏页卡片点击率',
        u'收藏页平均停留时间',
        u'收藏页PV',
        u'收藏页UV',
        u'整体PV',
        u'整体UV',
    ]