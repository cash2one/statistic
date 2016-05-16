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
        u'地区点击数': {
            'query': {
                'query.act': 'click',
                'query.extend.key': 'province',
            },
            'type': 'count',
        },
        u'地区点击率': {
            'numerator': u'地区点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'专业点击数': {
            'query': {
                'query.act': 'click',
                'query.extend.key': 'majorName',
            },
            'type': 'count',
        },
        u'专业点击率': {
            'numerator': u'专业点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'类别点击数': {
            'query': {
                'query.act': 'click',
                'query.extend.key': 'collegeType',
            },
            'type': 'count',
        },
        u'类别点击率': {
            'numerator': u'类别点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'标签点击数': {
          'query': {
                'query.act': 'click',
                'query.extend.key': 'eduProjectType',
            },
            'type': 'count',
        },
        u'标签点击率': {
            'numerator': u'标签点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'学校点击数': {
            'query': {
                'query.act': 'jump_college',
            },
            'type': 'count',
        },
        u'学校点击率': {
            'numerator': u'学校点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'志愿填报按钮点击数': {
            'query': {
                'query.act': 'jump_will',
            },
            'type': 'count',
        },
        u'志愿填报按钮点击率': {
            'numerator': u'志愿填报按钮点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'下载度秘点击数': {
            'query': {
                'query.act': 'download',
            },
            'type': 'count',
        },
        u'下载度秘点击率': {
            'numerator': u'下载度秘点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'关注点击数': {
            'query': {
                'query.act': 'fav_add',
            },
            'type': 'count',
        },
         u'关注点击率': {
            'numerator': u'关注点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
         u'取消关注点击数': {
            'query': {
                'query.act': 'fav_cancel',
            },
            'type': 'count',
        },
         u'取消关注点击率': {
            'numerator': u'取消关注点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'度秘查看全部链接点击数': {
            'query': {
                'query.act':'jump_college_all'
            },
            'type': 'count',
        },
        u'度秘查看全部链接点击率': {
            'numerator': u'度秘查看全部链接点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
        u'学校专业点击数': {
            'query': {
                'query.act': 'jump_major',
            },
            'type': 'count',
        },
         u'学校专业点击率': {
            'numerator': u'学校专业点击数',
            'denominator': u'PV',
            'type': 'percent',
        },
    }

    groups = [{
        'attribute': 'file_group',
        'type': 'file',
        'key': 'client',
    }, {
        'attribute': 'page_group',
        'type': 'index',
        'key': 'page',
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
                    'dumi_gaokao_search_college',#院校泛需求检索页面
                    'dumi_gaokao_search_college_brief',#院校泛需求推荐页面
                    'dumi_gaokao_recommend_college',#院校精确需求推荐页面
                    'dumi_gaokao_search_major_brief',#专业泛需求推荐页面
                    'dumi_gaokao_search_major',#专业泛需求列表页面
                    'dumi_gaokao_recommend_major',#专业精确需求推荐页面
                ],
            },
        },
        'index': [
            u'PV',
            u'UV',
        ],
    }, {
        'name': u'院校泛需求检索页面',
        'query': {
            'query.cat': 'dumi_gaokao_search_college',
        },
        'index': [
            u'PV',
            u'UV',
            u'地区点击率',
            u'专业点击率',
            u'类别点击率',
            u'标签点击率',
            u'学校点击率',
            u'志愿填报按钮点击率',
            u'下载度秘点击率',
            u'关注点击率',
            u'取消关注点击率',
        ],
    }, {
        'name': u'院校泛需求推荐页面',
        'query': {
            'query.cat': 'dumi_gaokao_search_college_brief',
        },
        'index': [
            u'PV',
            u'UV',
            u'学校点击率',            
            u'志愿填报按钮点击率',
            u'度秘查看全部链接点击率',
            u'关注点击率',
            u'取消关注点击率',
        ],
    }, {
        'name': u'院校精确需求推荐页面',
        'query': {
            'query.cat': 'dumi_gaokao_recommend_college',
        },
        'index': [
            u'PV',
            u'UV',
            u'学校专业点击率',
            u'志愿填报按钮点击率',
            u'度秘查看全部链接点击率',
            u'关注点击率',          
            u'取消关注点击率',
        ],
    }, {
        'name': u'专业泛需求推荐页面',
        'query': {
            'query.cat': 'dumi_gaokao_search_major_brief',
        },
        'index': [
            u'PV',
            u'UV',
            u'学校专业点击率',
            u'志愿填报按钮点击率',            
            u'度秘查看全部链接点击率',
        ],
    }, {
        'name': u'专业泛需求列表页面',
        'query': {
            'query.cat': 'dumi_gaokao_search_major',
        },
        'index': [
            u'PV',
            u'UV',            
            u'学校专业点击率',
            u'志愿填报按钮点击率',
        ],
    },{
        'name': u'专业精确需求推荐页面',
        'query': {
            'query.cat': 'dumi_gaokao_recommend_major',
        },
        'index': [
            u'PV',
            u'UV', 
            u'学校专业点击率',
            u'志愿填报按钮点击率',
            u'度秘查看全部链接点击率',
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
        u'地区点击率',
        u'专业点击率',
        u'类别点击率',       
        u'标签点击率',        
        u'学校点击率',        
        u'志愿填报按钮点击率',
        u'下载度秘点击率',      
        u'关注点击率',       
        u'取消关注点击率',        
        u'学校专业点击率',
        u'志愿填报按钮点击率',      
        u'度秘查看全部链接点击率',    
    ]
