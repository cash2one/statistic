# coding=utf-8
import os
import re
import copy
import types

from bson.code import Code

from conf import conf
from midpage import midpagedb

class MidpageProduct(object):
    def __init__(self, date):
        self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()

    def statist(self):
        raise NotImplementedError()

    def save_result(self, result):
        raise NotImplementedError()

    def run(self):
        self.save_result(self.statist())




class CRMMidpageProduct(object):
    defaul_query = {}

    index_map = { #example，具体类需要复写
        u'pv': {
            'query': {
                'query.cat':'dumi_meishi',
                'query.act':'pv',
            },
            'type': 'count',
        },
        u'图片点击数': {
            'query': {
                'query.cat':'dumi_meishi',
                'query.act':'b_click_focusimg',
            },
            'type': 'count',
        },
        u'图片点击率': {
            'numerator': u'图片点击数',
            'denominator': u'pv',
            'type': 'percent',
        },
        u'多个指标相加': {
            'index': ['pv', '图片点击数'],
            'type': 'add',
        },
        u'uv': {
            'query': {},
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'map reduce计算': {
            'query': {},
            'map': 'function () {}',
            'reduce': 'function (key, values) {}',
            'local': '<python function>',
            'type': 'map_reduce',
        },
        u'本地计算': {
            'query': {},
            'fields': {
                'timestamp': 1,
                'baiduid': 1,
            },
            'local': '<python function>',
            'type': 'output',
        },
        u'求和': {
            'query': {},
            'field': 'query.extend.card_num',
            'type': 'sum',
        },
        u'求平均值': {
            'query': {},
            'field': 'query.extend.card_num',
            'type': 'avg',
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
    ]

    def __init__(self, date):
        self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()

    def _percent_statist(self, key, value_map, index_map):
        self._statist(index_map[key]['numerator'], value_map, index_map)
        self._statist(index_map[key]['denominator'], value_map, index_map)
        numerator = value_map[index_map[key]['numerator']]
        denominator = value_map[index_map[key]['denominator']]
        value_map[key] = float(numerator)/denominator if denominator else 0

    def _add_statist(self, key, value_map, index_map):
        if type(index_map[key]['index']) == types.StringType:
            index_map[key]['index'] = [index_map[key]['index']]
        add_result = 0
        for add_index in index_map[key]['index']:
            self._statist(add_index, value_map, index_map)
            add_result += value_map[add_index]
        value_map[key] = add_result

    def _count_statist(self, key, value_map, index_map):
        value_map[key] = self.log_collection.find(index_map[key]['query']).count()

    def _distinct_count_statist(self, key, value_map, index_map):
        if type(index_map[key]['distinct_key']) == types.StringType:
            value_map[key] = len(self.log_collection.find(index_map[key]['query'])\
                .distinct(index_map[key]['distinct_key']))
        else:
            group = {}
            group['_id'] = {field: '$' + field for field in index_map[key]['distinct_key']}
            value = list(self.log_collection.aggregate([
                {'$match': index_map[key]['query']},
                {'$group': group},
                {'$group': {'_id': '', 'count': {'$sum': 1}}},
            ]))
            if value:
                value_map[key] = value[0]['count']
            else:
                value_map[key] = 0

    def _sum_statist(self, key, value_map, index_map):
        value = list(self.log_collection.aggregate([
            {'$match': index_map[key]['query']},
            {'$group': {'_id': '', 'sum': {'$sum': '$' + index_map[key]['field']}}},
        ]))
        if value:
            value_map[key] = value[0]['sum']
        else:
            value_map[key] = 0

    def _avg_statist(self, key, value_map, index_map):
        value = list(self.log_collection.aggregate([
            {'$match': index_map[key]['query']},
            {'$group': {'_id': '', 'avg': {'$avg': '$' + index_map[key]['field']}}},
        ]))
        if value:
            value_map[key] = value[0]['avg']
        else:
            value_map[key] = 0

    def _map_reduce_statist(self, key, value_map, index_map):
        results = self.log_collection.map_reduce(Code(index_map[key]['map']),\
            Code(index_map[key]['reduce']), "results", query=index_map[key]['query'])
        value_map[key] = index_map[key]['local'](results.find())

    def _output_statist(self, key, value_map, index_map):
        results = self.log_collection.find(index_map[key]['query'], index_map[key].get('fields'))
        value_map[key] = index_map[key]['local'](results)

    def _statist(self, key, value_map, index_map):
        if value_map.get(key) is not None:
            return
        if index_map[key]['type'] == '':
            raise Exception(u"未指定指标类型")
        try:
            func = getattr(self, '_' + index_map[key]['type'] + '_statist')
        except AttributeError:
            raise Exception(u"未知的指标类型")
        else:
            func(key, value_map, index_map)

    def make_regex(self, query):
        u"""
        解决正则表达式不能deepcopy的问题
        配置中正则表达式写作/reg/，deepcopy后编译
        """
        if type(query) == dict:
            for key in query:
                if type(query[key]) == dict or type(query[key]) == list:
                    self.make_regex(query[key])
                elif type(query[key]) == types.StringType:
                    if query[key].startswith('/') and query[key].endswith('/'):
                        query[key] = query[key][1:-1]
                        query[key] = re.compile(query[key])
        elif type(query) == list:
            for key in xrange(len(query)):
                if type(query[key]) == dict or type(query[key]) == list:
                    self.make_regex(query[key])
                elif type(query[key]) == types.StringType:
                    if query[key].startswith('/') and query[key].endswith('/'):
                        query[key] = query[key][1:-1]
                        query[key] = re.compile(query[key])

    def statist_group(self, match):
        index_map = copy.deepcopy(self.index_map)
        defaul_query = copy.deepcopy(self.defaul_query)
        self.make_regex(index_map)
        self.make_regex(defaul_query)
        #补充分组条件
        for key, value in index_map.items():
            if 'query' in value:
                value['query'].update(match)
                value['query'].update(defaul_query)

        value_map = {}
        #开始计算指标
        for key, value in index_map.items():
            self._statist(key, value_map, index_map)
        return value_map

    def statist(self):
        ret = {}
        if len(self.file_group) == 0:
            raise Exception(u"file group is empty!")
        if len(self.index_group) == 0:
            self.index_group = [{
                'name': 'total',
                'query': {}
            }]

        for fg in self.file_group:
            ret[fg['name']] = {}
            for ig in self.index_group:
                match = {}
                match.update(fg['query'])
                match.update(ig['query'])
                ret[fg['name']][ig['name']] = self.statist_group(match)
        return ret

    def _get_path(self):
        date = self.date
        module_name = self.__module__.split(".")[-1]
        output = os.path.join(conf.OUTPUT_DIR, "midpage/%s/%s" % (module_name, date))
        if not os.path.exists(output):
            os.makedirs(output)
        return output

    def write_result(self, filename, rows):
        num = 100
        with open(filename, 'w') as fp:
            for row in rows:
                row.insert(0, num)
                num += 1
                row = [unicode(r) for r in row]
                row = '\t'.join(row)
                fp.write(row.encode('utf-8'))
                fp.write('\n')

    def save_result(self, result):
        path = self._get_path()
        for fg in self.file_group:
            filename = os.path.join(path, "%s.txt" % fg['name'])
            file_result = result[fg['name']]
            rows = []
            for ig in self.index_group:
                index_result = file_result[ig['name']]
                for index in self.index_order:
                    row = [ig['name'], index]
                    row.append(index_result[index])
                    rows.append(row)
            self.write_result(filename, rows)

    def run(self):
        self.save_result(self.statist())
