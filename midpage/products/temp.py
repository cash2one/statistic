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
        ret = {}
        ret['movie'] = {'pv':'', 'uv':''}
        ret['travel'] = {'pv':'', 'uv':''}
        ret['food'] = {'pv':'', 'uv':''}

        temp_Db = midpagedb.DateLogDb()
        temp = temp_Db.get_collection()
        tempDb = temp_Db.get_db()
        #指标计算
        #movie
        SHOW_REG = re.compile(r"^/movie/\w+\.html")
        ret['movie']['pv'] = temp.find({'query.f':'dumi','url':SHOW_REG}).count()
        ret['movie']['uv'] = len(temp.find({'query.f':'dumi','url':SHOW_REG}).distinct('baiduid'))
        #travel
        SHOW_REG = re.compile(r"^/dumi/travel")
        ret['travel']['pv'] = temp.find({'url':SHOW_REG}).count()
        ret['travel']['uv'] = len(temp.find({'url':SHOW_REG}).distinct('baiduid'))
        #food
        SHOW_REG = re.compile(r"^/dumi/travel")
        ret['food']['pv'] = temp.find({'query.cat':'dumi_meishi'}).count()
        ret['food']['uv'] = len(temp.find({'query.cat':'dumi_meishi'}).distinct('baiduid'))

        return ret
    def save_result(self, result):
        u"""
        result为statist返回的结果，用于存储结果，可以存储到本地也可以存储如数据库
        """
        date = self.date
        output = conf.OUTPUT_DIR
        midpage_output = os.path.join(output, "midpage/temp/%s" % self.date)
        if not os.path.exists(midpage_output):
            os.makedirs(midpage_output)
        file_other = os.path.join(midpage_output, 'temp.txt')
        with open(file_other, 'w') as f:
            #count = 100
            for title in result:
                for index in result[title]:
                    out = '%s\t%s\t%s\r\n' % (title, index, result[title][index])
                    f.write(out)
                    #count = count + 1
