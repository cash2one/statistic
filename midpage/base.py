# coding=utf-8
import os
import copy

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
                'query.act':'pv'
            },
            'type': 'count'
        },
        u'图片点击数': {
            'query': {
                'query.cat':'dumi_meishi',
                'query.act':'b_click_focusimg'
            },
            'type': 'count'
        },
        u'图片点击率': {
            'numerator': u'图片点击数',
            'denominator': u'pv',
            'type': 'percent'
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
    ]

    def __init__(self, date):
        self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()

    def _percent_statist(self, key, value_map, index_map):
        if value_map.get(key) is not None:
            return
        self._count_statist(index_map[key]['numerator'], value_map, index_map)
        self._count_statist(index_map[key]['denominator'], value_map, index_map)
        numerator = value_map[index_map[key]['numerator']]
        denominator = value_map[index_map[key]['denominator']]
        value_map[key] = float(numerator)/denominator if denominator else 0

    def _count_statist(self, key, value_map, index_map):
        if value_map.get(key) is not None:
            return
        value_map[key] = self.log_collection.find(index_map[key]["query"]).count()

    def statist_group(self, match):
        index_map = copy.deepcopy(self.index_map)
        defaul_query = copy.deepcopy(self.defaul_query)
        #补充分组条件
        for key, value in index_map.items():
            if 'query' in value:
                value['query'].update(match)
                value['query'].update(defaul_query)

        value_map = {}
        #开始计算指标
        for key, value in index_map.items():
            if value['type'] == 'count':
                self._count_statist(key, value_map, index_map)
            elif value['type'] == 'percent':
                self._percent_statist(key, value_map, index_map)
            else:
                raise Exception(u"未知的指标类型")
        return value_map

    def statist(self):
        ret = {}
        if len(self.file_group) == 0:
            raise Exception(u"file group is empty!")
        if len(self.index_group) == 0:
            self.index_group = [{
                'name': '',
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