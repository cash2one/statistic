# coding=utf-8
import os
import re
import copy
import types

from bson.code import Code

from conf import conf
from lib import tools
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
        u'插值': {
            'minuend': u'被减数',
            'subtrahend': u'减数',
            'type': 'sub',
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

    groups = [{
        'attribute': 'file_group',
        'type': 'file',
        'key': 'client',
    }, {
        'attribute': 'index_group',
        'type': 'index',
        'key': 'os',
    }]

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
    }, {
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

    def _sub_statist(self, key, value_map, index_map):
        self._statist(index_map[key]['minuend'], value_map, index_map)
        self._statist(index_map[key]['subtrahend'], value_map, index_map)
        minuend = value_map[index_map[key]['minuend']]
        subtrahend = value_map[index_map[key]['subtrahend']]
        value_map[key] = minuend - subtrahend

    def _count_statist(self, key, value_map, index_map):
        value_map[key] = self.log_collection.find(index_map[key]['query']).count()

    def _distinct_count_statist(self, key, value_map, index_map):
        if type(index_map[key]['distinct_key']) == types.StringType:
            index_map[key]['distinct_key'] = [index_map[key]['distinct_key']]
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
        u"""
        统计类型总入口，具体各个类型的统计走_[type name]_statist方法
        """
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

    def statist_group(self, match, keys=None):
        u"""统计单个分组
        """
        index_map = copy.deepcopy(self.index_map)
        defaul_query = copy.deepcopy(self.defaul_query)
        self.make_regex(index_map)
        self.make_regex(defaul_query)
        if keys is None:
            keys = index_map.keys()
        else:
            try:
                index_map = {key:index_map[key] for key in keys}
            except KeyError as e:
                tools.log('[ERROR]index not exists:%s' % e.message)
        #补充分组条件
        for key in keys:
            value = index_map[key]
            if 'query' in value:
                value['query'].update(match)
                value['query'].update(defaul_query)

        value_map = {}
        #开始计算指标
        for key in keys:
            value = index_map[key]
            self._statist(key, value_map, index_map)
        return value_map

    def _iter_groups(self, layer=0):
        u"""根据self.groups，转换出各个分组
        """
        total_layer = len(self.groups) - 1
        group_info = self.groups[layer]
        group_queries = getattr(self, group_info['attribute'])
        if len(group_queries) == 0:
            raise Exception("[ERROR]group can't be empty:%s" % group_info['attribute'])
        for g in group_queries:
            ret = [copy.deepcopy(g)]
            if layer == total_layer:
                yield ret
            else:
                for ext in self._iter_groups(layer + 1):
                    tmp = copy.deepcopy(ret)
                    yield tmp + ext

    def statist(self):
        u"""统计入口
        输出形式：
        {
            'group01 name': {
                'group11 name': {index map...},
                'group12 name': {index map...},
            },
        }
        """
        ret = {}
        for group in self._iter_groups():
            match = {}
            keys = None
            for g in group:
                match.update(g['query'])
                if g.get('index') is not None:
                    if keys is None:
                        keys = g['index']
                    else:
                        keys = list(set(keys) & set(g['index']))
            tmp_ret = ret
            group_len = len(group) - 1
            for i, g in enumerate(group):
                if i == group_len:
                    tmp_ret[g['name']] = self.statist_group(match, keys)
                    break
                elif tmp_ret.get(g['name']) is None:
                    tmp_ret[g['name']] = {}
                tmp_ret = tmp_ret[g['name']]
        return ret

    def _get_path(self):
        date = self.date
        module_name = self.__module__.split(".")[-1]
        output = os.path.join(conf.OUTPUT_DIR, "midpage/%s/%s" % (module_name, date))
        if not os.path.exists(output):
            os.makedirs(output)
        return output

    def write_result(self, filename, rows):
        u"""将结果输出到文件
        """
        num = 100
        with open(filename, 'w') as fp:
            for row in rows:
                row.insert(0, num)
                num += 1
                row = [unicode(r) for r in row]
                row = '\t'.join(row)
                fp.write(row.encode('utf-8'))
                fp.write('\n')

    def get_rows(self, result, group, filename):
        u"""将一个分组的输出转换成数组形式，便于后续输出到文件
        """
        rows = []
        group_name = []
        index_result = result
        for g in group:
            group_name.append(g['name'])
            index_result = index_result[g['name']]
        for index in self.index_order:
            if index in index_result:
                group_name = copy.deepcopy(group_name)
                row = group_name + [index]
                row.append(index_result[index])
                rows.append(row)
            else:
                group_sign = [g['name'] for g in group]
                group_sign = ','.join(group_sign)
                tools.log('[NOTICE]index not exist, index:%s, group:%s, file:%s' %\
                    (index, group_sign, filename))
        return rows

    def save_result(self, result):
        path = self._get_path()
        file_sign = self.groups[0]['type'] == 'file'
        file_index = {}
        for group in self._iter_groups():
            if not file_sign:
                filename = "total"
                _group = group
                _result = result
            else:
                filename = group[0]['name']
                _group = group[1:]
                _result = result[filename]
            rows = self.get_rows(_result, _group, filename)
            if file_index.get(filename) is None:
                file_index[filename] = rows
            else:
                file_index[filename].extend(rows)

        for filename, rows in file_index.items():
            filename = os.path.join(path, '%s.txt' % filename)
            self.write_result(filename, rows)

    def run(self):
        self.save_result(self.statist())
