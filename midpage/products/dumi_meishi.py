# coding=utf-8
import os

from midpage import base

source = 'qianxun'



class Product(base.CRMMidpageProduct):
    defaul_query = {
        'source': 'qianxun',
    }

    index_map = { #example，具体类需要复写
        u'PV': {
            'query': {
                'query.act': 'pv',
            },
            'type': 'count',
        },
        u'UV': {
            'query': {
                'query.act': 'pv'
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'UPV': {
            'query': {
                'query.act': 'pv',
            },
            'distinct_key': ['baiduid', 'query.meishi_id'],
            'type': 'distinct_count',
        },
        u'图片点击数': {
            'query': {
                'query.act': 'b_click_focusimg',
            },
            'type': 'count',
        },
        u'图片点击率': {
            'numerator': u'图片点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'图片滑动数': {
            'query': {
                'query.act': 'b_slide_focusimg',
            },
            'type': 'count',
        },
        u'图片滑动率': {
            'numerator': u'图片滑动数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'电话点击数': {
            'query': {
                'query.act': 'b_click_tel',
            },
            'type': 'count',
        },
        u'电话点击率': {
            'numerator': u'电话点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'地址点击数': {
            'query': {
                'query.act': 'a_click_location',
            },
            'type': 'count',
        },
        u'地址点击率': {
            'numerator': u'地址点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'评论点击数': {
            'query': {
                'query.act': 'a_click_comment',
            },
            'type': 'count',
        },
        u'评论点击率': {
            'numerator': u'评论点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'团购信息总点击数': {
            'query': {
                'query.act': 'a_click_groupon_item',
            },
            'type': 'count',
        },
        u'团购信息点击数': {
            'query': {
                'query.act': 'a_click_groupon_item',
                '$or':[{
                    'query.xpath':'div-div2-div3-section5-div2-a-div-div(link)'
                }, {
                    'query.xpath':'div-div2-div3-section5-div-a-div-div(link)'
                }],
            },
            'type': 'count',
        },
        u'团购信息点击率': {
            'numerator': u'团购信息点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'其他团购信息点击数': {
            'minuend': u'团购信息总点击数',
            'subtrahend': u'团购信息点击数',
            'type': 'sub',
        },
        u'其他团购信息点击率': {
            'numerator': u'其他团购信息点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'图片展现数': {
            'query': {
                'query.act':'pv'
            },
            'field': 'query.image_num',
            'type': 'sum',
        },
        u'评论展现量': {
            'query': {
                'query.act':'pv'
            },
            'field': 'query.review_num',
            'type': 'sum',
        },
        u'团购信息展现数': {
            'query': {
                'query.act':'pv'
            },
            'field': 'query.tuangou_num',
            'type': 'sum',
        },
        u'展现电话次数': {
            'query': {
                'query.act': 'pv',
                'query.has_phone': '1',
            },
            'type': 'count',
        },
        u'展现地址次数': {
            'query': {
                'query.act': 'pv',
                'query.has_address': '1',
            },
            'type': 'count',
        },
        u'榜单模块点击数': {
            'query': {
                'query.act': 'a_click_toplist',
            },
            'type': 'count',
        },
        u'榜单模块点击率': {
            'numerator': u'榜单模块点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'菜品推荐模块点击数': {
            'query': {
                'query.act': 'a_click_recommend_food',
            },
            'type': 'count',
        },
        u'菜品推荐模块点击率': {
            'numerator': u'菜品推荐模块点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'相关评价tag整体点击数': {
            'query': {
                'query.act': 'b_click_comment_tag',
            },
            'type': 'count',
        },
        u'相关评价tag整体点击率': {
            'numerator': u'相关评价tag整体点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'餐厅点击数': {
            'query': {
                'query.act': 'a_click_rank_item',
            },
            'type': 'count',
        },
        u'餐厅点击率': {
            'numerator': u'餐厅点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'餐厅展现数': {
            'query': {
                'query.act': {
                    '$in': ['pv', 'load_more'],
                },
            },
            'field': 'query.extend.restaurant_num',
            'type': 'sum',
        },
        u'餐厅点展比': {
            'numerator': u'餐厅点击数',
            'denominator': u'餐厅展现数',
            'type': 'percent',
        },
        u'榜单卡片点击数': {
            'query': {
                'query.act': 'a_click_toplist_item',
            },
            'type': 'count',
        },
        u'榜单卡片展现数': {
            'query': {
                'query.act': {
                    '$in': ['pv', 'load_more'],
                },
            },
            'field': 'query.extend.rank_num',
            'type': 'sum',
        },
        u'榜单卡片点展比': {
            'numerator': u'榜单卡片点击数',
            'denominator': u'榜单卡片展现数',
            'type': 'percent',
        },
        u'榜单卡片点击率': {
            'numerator': u'榜单卡片点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'回到顶部icon点击数': {
            'query': {
                'query.act': 'b_click_top',
            },
            'type': 'count',
        },
        u'回到顶部icon点击率': {
            'numerator': u'回到顶部icon点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'页面平均停留时间': {
            'query': {
                'query.act':'stay_time',
                'query.duration': {'$lt': 1800},
            },
            'field': 'query.duration',
            'type': 'avg',
        },
    }

    groups = [{
        'attribute': 'file_group',
        'type': 'file',
        'key': 'client',
    }, {
        'attribute': 'page_group',
        'type': 'index',
        'key': 'type',
    }, {
        'attribute': 'index_group',
        'type': 'index',
        'key': 'os',
    }]

    page_group = [{
        'name': u'总体',
        'query': {
            'query.cat':{
                '$in': [
                    'dumi_meishi',#餐厅落地页
                    'dumi_meishi_recommend_food',#菜品推荐页
                    'dumi_meishi_toplist',#榜单详情页
                    'dumi_meishi_restaurant',#餐厅列表页
                    'dumi_meishi_ranks',#榜单列表页
                ],
            },
        },
        'index': [
            u'PV',
            u'UV',
        ],
    }, {
        'name': u'餐厅落地页',
        'query': {
            'query.cat': 'dumi_meishi',
        },
        'index': [
            u'PV',
            u'UV',
            u'UPV',
            u'图片点击数',
            u'图片点击率',
            u'图片滑动数',
            u'图片滑动率',
            u'电话点击数',
            u'电话点击率',
            u'地址点击数',
            u'地址点击率',
            u'评论点击数',
            u'评论点击率',
            u'团购信息总点击数',
            u'团购信息点击数',
            u'团购信息点击率',
            u'其他团购信息点击数',
            u'其他团购信息点击率',
            u'图片展现数',
            u'评论展现量',
            u'团购信息展现数',
            u'展现电话次数',
            u'展现地址次数',
            u'榜单模块点击数',
            u'榜单模块点击率',
            u'菜品推荐模块点击数',
            u'菜品推荐模块点击率',
            u'相关评价tag整体点击数',
            u'相关评价tag整体点击率',
            u'页面平均停留时间',
        ],
    }, {
        'name': u'菜品推荐页',
        'query': {
            'query.cat': 'dumi_meishi_recommend_food',
        },
        'index': [
            u'PV',
            u'UV',
            u'页面平均停留时间',
        ],
    }, {
        'name': u'榜单详情页',
        'query': {
            'query.cat': 'dumi_meishi_toplist',
        },
        'index': [
            u'PV',
            u'UV',
            u'餐厅点击数',
            u'餐厅点击率',
            u'餐厅展现数',
            u'餐厅点展比',
            u'页面平均停留时间',
        ],
    }, {
        'name': u'餐厅列表页',
        'query': {
            'query.cat': 'dumi_meishi_restaurant',
        },
        'index': [
            u'PV',
            u'UV',
            u'餐厅点击数',
            u'餐厅点击率',
            u'餐厅展现数',
            u'餐厅点展比',
            u'榜单卡片点击数',
            u'榜单卡片展现数',
            u'榜单卡片点展比',
            u'回到顶部icon点击数',
            u'回到顶部icon点击率',
            u'页面平均停留时间',
        ],
    }, {
        'name': u'榜单列表页',
        'query': {
            'query.cat': 'dumi_meishi_ranks',
        },
        'index': [
            u'PV',
            u'UV',
            u'榜单卡片点击数',
            u'榜单卡片展现数',
            u'榜单卡片点展比',
            u'榜单卡片点击率',
            u'页面平均停留时间',
        ],
    }]

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
        u'PV',
        u'UV',
        u'UPV',
        u'图片点击率',
        u'图片滑动率',
        u'电话点击率',
        u'地址点击率',
        u'评论点击率',
        u'团购信息点击率',
        u'其他团购信息点击率',
        u'图片展现数',
        u'评论展现量',
        u'团购信息展现数',
        u'展现电话次数',
        u'展现地址次数',
        u'榜单模块点击率',
        u'菜品推荐模块点击率',
        u'相关评价tag整体点击率',
        u'餐厅点击率',
        u'餐厅点展比',
        u'榜单卡片点展比',
        u'榜单卡片点击率',
        u'回到顶部icon点击率',
        u'页面平均停留时间',
    ]
