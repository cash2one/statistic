# coding=utf-8
import os
import re
from conf import conf
from midpage import base
from midpage import midpagedb

class Product(base.MidpageProduct):
    def template(self):
        ret = {
            'show_pv':0,
            'play_pv':0,
            'show_uv':0,
            'play_uv':0,
            'play_rate':0,
            'play_per_capita':0
        }
        return ret

    def statist(self):
        u"""
        self.log_collection可以拿到mongo中的日志集合，用于统计指标的函数
        """
        SHOW_REG = re.compile(r"^/movie/\w+\.html")
        PLAY_REG = re.compile(r"/m\.gif")
        other = {}
        other['other'] = self.template()
        other['ios'] = self.template()
        other['android'] = self.template()
        other['total'] = self.template()
        na = {}
        na['other'] = self.template()
        na['ios'] = self.template()
        na['android'] = self.template()
        na['total'] = self.template()
        mb = {}
        mb['other'] = self.template()
        mb['ios'] = self.template()
        mb['android'] = self.template()
        mb['total'] = self.template()
        total = {}
        total['other'] = self.template()
        total['ios'] = self.template()
        total['android'] = self.template()
        total['total'] = self.template()
        ret = {}
        ret['other'] = other
        ret['NA'] = na
        ret['MB'] = mb
        ret['total'] = total
        #连接数据库
        movie_Db = midpagedb.DateLogDb()
        movie = movie_Db.get_collection()
        movieDb = movie_Db.get_db()
        #show_pv
        show_pv = movie.aggregate([
            {'$match':{'query.f':'dumi','url':SHOW_REG}},
            {'$group':{'_id':{'os':"$os",'client':"$client"},'count':{'$sum':1}}}
        ]);
        for obj in show_pv:
            ret[obj['_id']['client']][obj['_id']['os']]['show_pv']= obj['count']
            ret[obj['_id']['client']]['total']['show_pv'] = ret[obj['_id']['client']]['total']['show_pv'] + obj['count']
            ret['total'][obj['_id']['os']]['show_pv'] = ret['total'][obj['_id']['os']]['show_pv'] + obj['count']
            ret['total']['total']['show_pv'] = ret['total']['total']['show_pv'] + obj['count']
        #play_pv
        play_pv = movie.aggregate([
            {'$match':{'query.t':'click','url':PLAY_REG,'query.e':'online_play','referr_query.f':'dumi'}},
            {'$group':{'_id':{'os':"$os",'client':"$client"},'count':{'$sum':1}}}
        ]);
        for obj in play_pv:
            ret[obj['_id']['client']][obj['_id']['os']]['play_pv']= obj['count']
            ret[obj['_id']['client']]['total']['play_pv'] = ret[obj['_id']['client']]['total']['play_pv'] + obj['count']
            ret['total'][obj['_id']['os']]['play_pv'] = ret['total'][obj['_id']['os']]['play_pv'] + obj['count']
            ret['total']['total']['play_pv'] = ret['total']['total']['play_pv'] + obj['count']
        #show_uv
        show_uv = movie.aggregate([
            {'$match':{'query.f':'dumi','url':SHOW_REG}},
            {'$group':{'_id':{'os':"$os",'client':"$client",'baiduid':"$baiduid"},'count':{'$sum':1}}},
            {'$group':{'_id':{'os':"$_id.os",'client':"$_id.client"},'count':{'$sum':1}}}
        ]);
        for obj in show_uv:
            ret[obj['_id']['client']][obj['_id']['os']]['show_uv']= obj['count']
            ret[obj['_id']['client']]['total']['show_uv'] = ret[obj['_id']['client']]['total']['show_uv'] + obj['count']
            ret['total'][obj['_id']['os']]['show_uv'] = ret['total'][obj['_id']['os']]['show_uv'] + obj['count']
            ret['total']['total']['show_uv'] = ret['total']['total']['show_uv'] + obj['count']
        #play_uv
        play_uv = movie.aggregate([
            {'$match':{'query.t':'click','url':PLAY_REG,'query.e':'online_play','referr_query.f':'dumi'}},
            {'$group':{'_id':{'os':"$os",'client':"$client",'baiduid':"$baiduid"},'count':{'$sum':1}}},
            {'$group':{'_id':{'os':"$_id.os",'client':"$_id.client"},'count':{'$sum':1}}}
        ]);
        for obj in play_uv:
            ret[obj['_id']['client']][obj['_id']['os']]['play_uv']= obj['count']
            ret[obj['_id']['client']]['total']['play_uv'] = ret[obj['_id']['client']]['total']['play_uv'] + obj['count']
            ret['total'][obj['_id']['os']]['play_uv'] = ret['total'][obj['_id']['os']]['play_uv'] + obj['count']
            ret['total']['total']['play_uv'] = ret['total']['total']['play_uv'] + obj['count']
        #play_rate
        for client in ret:
            for oprate_sys in ret[client]:
                if ret[client][oprate_sys]['show_pv'] != 0:
                    ret[client][oprate_sys]['play_rate'] = float(ret[client][oprate_sys]['play_pv'])/ret[client][oprate_sys]['show_pv']
                if ret[client][oprate_sys]['show_uv'] != 0:
                    ret[client][oprate_sys]['play_per_capita'] = float(ret[client][oprate_sys]['play_pv'])/ret[client][oprate_sys]['show_uv']    
        return ret

    def save_result(self, result):
        u"""
        result为statist返回的结果，用于存储结果，可以存储到本地也可以存储如数据库
        """
        date = self.date
        output = conf.OUTPUT_DIR
        midpage_output = os.path.join(output, "midpage/movie/%s" % self.date)
        if not os.path.exists(midpage_output):
            os.makedirs(midpage_output)
        file_other = os.path.join(midpage_output, 'other.txt')
        with open(file_other, 'w') as f:
            count = 100
            for operate_system in result['other']:
                for title in result['other'][operate_system]:
                     f.write('%s\tother\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['other'][operate_system][title]))
                     count = count + 1

        file_na = os.path.join(midpage_output, 'NA.txt')
        with open(file_na, 'w') as f:
            count = 100
            for operate_system in result['NA']:
                for title in result['NA'][operate_system]:
                     f.write('%s\tNA\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['NA'][operate_system][title]))
                     count = count + 1

        file_mb = os.path.join(midpage_output, 'MB.txt')
        with open(file_mb, 'w') as f:
            count = 100
            for operate_system in result['MB']:
                for title in result['MB'][operate_system]:
                     f.write('%s\tMB\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['MB'][operate_system][title]))
                     count = count + 1

        file_mb = os.path.join(midpage_output, 'total.txt')
        with open(file_mb, 'w') as f:
            count = 100
            for operate_system in result['total']:
                for title in result['total'][operate_system]:
                     f.write('%s\ttotal\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['total'][operate_system][title]))
                     count = count + 1