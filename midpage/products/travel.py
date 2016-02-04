# coding=utf-8
import os
import re
from conf import conf
from midpage import base
from midpage import midpagedb
import json

class Product(base.MidpageProduct):
    def statist(self):
        u"""
        self.log_collection可以拿到mongo中的日志集合，用于统计指标的函数
        """
        SHOW_REG = re.compile(r"^/dumi/travel")
        PLAY_REG = re.compile(r"/m\.gif")
        ret = {}
        query = []
        page = []
        ret['query'] = query
        ret['page'] = page
        #连接数据库
        travel_Db = midpagedb.DateLogDb()
        travel = travel_Db.get_collection()
        travelDb = travel_Db.get_db()
        #query
        query_info = travel.aggregate([
            {'$match':{'url':SHOW_REG}},
            {'$group':{'_id':{'title':"$query.title",'os':"$os", 'origin_query':'$query.origin_query'},'count':{'$sum':1}}}
        ]);
        for obj in query_info:
            ret['query'].append(obj)
        #page
        page_info = travel.aggregate([
            {'$match':{'url':PLAY_REG, 'query.e' : 'dumi_travel_list', 'query.t' : 'click'}},
            {'$group':{'_id':{'i':"$query.i",'os':"$os", 'p':'$query.p'},'count':{'$sum':1}}}
        ]);
        for obj in page_info:
            ret['page'].append(obj)

        return ret

    def save_result(self, result):
        u"""
        result为statist返回的结果，用于存储结果，可以存储到本地也可以存储如数据库
        """
        date = self.date
        output = conf.OUTPUT_DIR
        midpage_output = os.path.join(output, "midpage/travel/%s" % self.date)
        if not os.path.exists(midpage_output):
            os.makedirs(midpage_output)
        file_other = os.path.join(midpage_output, 'query.txt')
        with open(file_other, 'w') as f:
            #count = 100
            for obj in result['query']:
                if '_id' in obj and 'title' in obj['_id'] and 'origin_query' in obj['_id'] and 'os' in obj['_id']:
                    out = u'%s\t%s\t%s\t%s\r\n' % (obj['_id']['title'], obj[u'_id'][u'origin_query'], obj[u'_id'][u'os'], obj['count'])
                    f.write(out.encode('utf-8'))
                    #count = count + 1

        file_na = os.path.join(midpage_output, 'page.txt')
        with open(file_na, 'w') as f:
            #count = 100
            for obj in result['page']:
                out = u'%s\t%s\t%s\t%s\r\n' % (obj['_id']['i'], obj['_id']['os'], obj['_id']['p'], obj['count'])
                f.write(out.encode('utf-8'))
                #count = count + 1
