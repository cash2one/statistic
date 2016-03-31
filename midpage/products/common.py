# coding=utf-8
import os
import re
from conf import conf
from midpage import base
from midpage import db
from midpage import midpagedb
import json

class Product(base.MidpageProduct):
    def __init__(self, date):
        self.date = date
        self.temp_Db = midpagedb.DateLogDb()
        self.db = self.temp_Db.get_collection()


    def initDict(self):
        return {'pv': 0, 'uv': 0, 'recall_pagenum': 0, 'list_pv': 0, 'entity_pv': 0}

    def getSearcher(self, source=None):
        u"""
            获取pv计算条件
            source: ps|dumi
        """
        exp_ua = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre'
        agent_filter = {'$regex': re.compile(r"^(?!.*(Baiduspider|YisouSpider|Googlebot)).*$"), '$nin':['-', exp_ua]}
        #异常请求,已与晓波确认exp_ua需要过滤掉
        exp_filter = {'user_agent': exp_ua, 'cookie': {'$nin':['-', '']}, 'referr': {'$nin':['-', '']}}
        searcher = {}
        search_dict = {}
        search_dict['ps'] = {}
        search_dict['dumi'] = {}
        search_dict['ps'][u'影视'] = {'query.f':{'$nin':['dumi']},'url': re.compile(r"^/movie/\w+_[0-9]+\.html")}
        search_dict['ps'][u'美食'] = {'url': re.compile(r"^/mid/\w+\.*"), 'query.type': 'restaurant'}
        search_dict['ps'][u'规章'] = {'url': re.compile(r"^/guizhang/(index|search)\.html.*")}
        search_dict['ps'][u'其他通用'] = {'url': re.compile(r"^/seagull/\w+\.*")}
        search_dict['dumi'][u'影视'] = {'query.f':'dumi','url': re.compile(r"^/movie/\w+_[0-9]+\.html")}
        search_dict['dumi'][u'美食'] = {'query.cat':{'$in': ['dumi_meishi','dumi_meishi_toplist','dumi_meishi_recommend_food']}}
        search_dict['dumi'][u'旅游'] = {'url': re.compile(r"^/dumi/travel")}

        #添加异常过滤条件
        filter_search_dict = {}
        for src in search_dict:
            if src not in filter_search_dict:
                filter_search_dict[src] = {}
            for fd in search_dict[src]:
                if fd not in filter_search_dict[src]:
                    filter_search_dict[src][fd] = {}
                temp_1 = search_dict[src][fd]
                temp_1['user_agent'] = agent_filter
                temp_2 = search_dict[src][fd]
                temp_2 = {'$and':[temp_2, exp_filter]}
                filter_search_dict[src][fd] = {'$or': [temp_1, temp_2]}

        if source is not None and filter_search_dict.has_key(source):
            searcher = filter_search_dict[source]
        else:
            searcher = filter_search_dict

        return searcher

    def getPageClassify(self, type):
        u"""优先匹配all，若无all则ps、dumi配置生效"""
        classify = {}
        classify['list'] = {}
        classify['entity'] = {}
        classify['list'][u'影视'] = {'all': {'url': re.compile(r"^/movie/list_\w+\.html")}}
        classify['entity'][u'影视'] = {'all': {'url': re.compile(r"^/movie/card_\w+\.html")}}
        classify['list'][u'美食'] = {'ps': {'url': re.compile(r"^/mid/list.*")}, 'dumi': {'query.cat':{'$in': ['dumi_meishi_toplist']}}}
        classify['entity'][u'美食'] = {'ps': {'url': re.compile(r"^/mid/detail.*")}, 'dumi': {'query.cat':{'$in': ['dumi_meishi']}}}
        classify['list'][u'其他通用'] = {'all': {'url': re.compile(r"^/seagull/list.*")}}
        classify['entity'][u'其他通用'] = {'all': 0}
        classify['list'][u'旅游'] = {'all': {}}
        classify['entity'][u'旅游'] = {'all': 0}
        classify['list'][u'规章'] = {'all': 0}
        classify['entity'][u'规章'] = {'ps': {'url': re.compile(r"^/guizhang/index\.html.*")}}

        return classify[type]

    def getSearchCondition(self, source, type=None, client=None, field=None):
        u"""
            source: ps|dumi
            type: all|entity|list
            client: PC|WISE|NA|MB
            field: 影视|美食|旅游|规章
        """
        search_dict = self.getSearcher(source)
        se_classify = None
        if type in ['entity', 'list']:
            se_classify = self.getPageClassify(type)

        searcher = []
        if field is not None:
            for fd in field:
                temp = {}
                if search_dict.has_key(fd):
                    temp = search_dict[fd]
                searcher.append(self._merge(temp, fd, source, se_classify))
        else:
            for fd in search_dict:
                temp = search_dict[fd]
                searcher.append(self._merge(temp, fd, source, se_classify))

        ors = []
        for i in searcher:
            temp = i
            if client is not None:
                if source == 'ps':
                    #大搜来源采用os区分
                    if client == "PC":
                        temp['os'] = 'other'
                    else:
                        temp['os'] = {'$nin':['other']}
                else:
                    #度秘来源采用client区分
                    temp['client'] = client 
            ors.append(temp) 

        return ors

    def _merge(self, target, fd, src, classify):
        u"""
            从classify中根据fd+src选取条件merge入target
            target: object
            fd: 美食|旅游等
            src: ps|dumi
            classify: object
        """
        se_cons = None
        if classify is not None and (fd in classify):
            if 'all' in classify[fd]:
                se_cons = classify[fd]['all']
            elif src in classify[fd]:
                se_cons = classify[fd][src]
        if se_cons is not None and isinstance(se_cons, int):
            target = {'$and': [target, {'1':0}]}
        elif se_cons is not None and isinstance(se_cons, object):
            target = {'$and': [target, se_cons]} 

        return target

    def _statist(self, source, client=None, field=None):
        ret = self.initDict()
        search_cons = {'$or': self.getSearchCondition(source, 'all', client, field)}
        ret['pv'] = self.db.count(search_cons)
        ret['uv'] = len(self.db.find(search_cons).distinct('baiduid'))
        search_entity_cons = {'$or': self.getSearchCondition(source, 'entity', client, field)}
        search_list_cons = {'$or': self.getSearchCondition(source, 'list', client, field)}
        ret['entity_pv'] = self.db.count(search_entity_cons)
        ret['list_pv'] = self.db.count(search_list_cons)
        ret['recall_pagenum'] = self.temp_Db.distinct_count(['url', 'query'], search_cons)

        return ret
   
    def getAll(self, fd=None):
        ret = self.initDict()
        search_dict = self.getSearcher()
        se_entity_classify = self.getPageClassify('entity')
        se_list_classify = self.getPageClassify('list')

        ors = []
        ors_entity = []
        ors_list = []
        for src in search_dict:
            for field in search_dict[src]:
                if fd is not None and field not in fd:
                    continue
                temp = {}
                temp = search_dict[src][field]
                ors.append(temp)
                ors_entity.append(self._merge(temp, field, src, se_entity_classify))
                ors_list.append(self._merge(temp, field, src, se_list_classify)) 
        search_cons = {'$or': ors}
        ret['pv'] = self.db.count(search_cons)
        ret['uv'] = len(self.db.find(search_cons).distinct('baiduid'))
        search_entity_cons = {'$or': ors_entity}
        search_list_cons = {'$or': ors_list}
        ret['entity_pv'] = self.db.count(search_entity_cons)
        ret['list_pv'] = self.db.count(search_list_cons)
        ret['recall_pagenum'] = self.temp_Db.distinct_count(['url', 'query'], search_cons)
        
        return ret

    def statist(self):
        u"""
        self.log_collection可以拿到mongo中的日志集合，用于统计指标的函数
        """
        ret = {'pc': {}, 'wise': {}, 'dumi_mb': {}, 'dumi_na': {}, 'all': {}, 'dumi': {}, 'ps': {}}
        #搜索端指标
        ret['pc'][u'影视'] = self._statist('ps', 'PC', [u'影视'])
        ret['pc'][u'美食(自然结果)'] = self._statist('ps', 'PC', [u'美食'])
        ret['wise'][u'规章'] = self._statist('ps', 'WISE', [u'规章'])
        ret['wise'][u'美食'] = self._statist('ps', 'WISE', [u'美食'])
        ret['wise'][u'影视'] = self._statist('ps', 'WISE', [u'影视'])
        ret['wise'][u'其他通用'] = self._statist('ps', 'WISE', [u'其他通用'])
        #度秘端指标
        ret['dumi_na'][u'影视'] = self._statist('dumi', 'NA', [u'影视'])
        ret['dumi_na'][u'美食'] = self._statist('dumi', 'NA', [u'美食'])
        ret['dumi_na'][u'旅游'] = self._statist('dumi', 'NA', [u'旅游'])
        ret['dumi_mb'][u'影视'] = self._statist('dumi', 'MB', [u'影视'])
        ret['dumi_mb'][u'美食'] = self._statist('dumi', 'MB', [u'美食'])
        ret['dumi_mb'][u'旅游'] = self._statist('dumi', 'MB', [u'旅游'])
        #整体指标
        ret['pc'][u'总体'] = self._statist('ps', 'PC', [u'影视', u'美食'])
        ret['wise'][u'总体'] = self._statist('ps', 'WISE', [u'影视', u'美食', u'规章', u'其他通用'])
        ret['dumi_na'][u'总体'] = self._statist('dumi', 'NA')
        ret['dumi_mb'][u'总体'] = self._statist('dumi', 'MB')
        ret['all'][u'总体'] = self.getAll()
        ret['all'][u'影视'] = self.getAll([u'影视'])
        ret['all'][u'美食'] = self.getAll([u'美食'])
        ret['all'][u'旅游'] = self.getAll([u'旅游'])    
        ret['all'][u'规章'] = self.getAll([u'规章'])
        ret['all'][u'其他通用'] = self.getAll([u'其他通用'])
        ret['dumi'][u'总体'] = self._statist('dumi')
        ret['ps'][u'总体'] = self._statist('ps')

        return ret

    def get_midpage_product(self):
        '''
        获取维度信息：方向(spo)、类目、端(PC/WISE/DM NA/DM手百)
        '''
        pdict = {}
        stat_db = db.SelectDataBase()
        result = stat_db.get_midpage_product()

        for i in result:
            if not pdict.has_key(i[2]):
                pdict[i[2]] = {}
            if i[1] not in pdict[i[2]]:
                pdict[i[2]][i[1]] = {}
            pdict[i[2]][i[1]] = {'id': i[0]}

        return pdict


    def save_result(self, result):
        u"""
        result为statist返回的结果，用于存储结果，可以存储到本地也可以存储如数据库
        """
        midpage = self.get_midpage_product()
        stat_db = db.SaveDataBase()
        index_key = self.initDict().keys()
        stat_db.clear_midpage_daily_summary(self.date, index_key)
        data = []
        if result:
            for side in result:
                for product in result[side]:
                    for index in result[side][product]:
                        temp = {}
                        temp['side'] = side
                        temp['product'] = product
                        pid = 0
                        if midpage.has_key(side) and midpage[side].has_key(product):
                            pid = midpage[side][product]['id']
                        temp['pid'] = pid
                        temp['index'] = index
                        temp['date'] = self.date
                        temp['value'] = result[side][product][index]
                        temp['last_modify_date'] = self.date
                        data.append(temp)
        stat_db.save_spo_index_info(data) 
