# coding=utf-8
import os

from conf import conf
from midpage import base
from midpage import midpagedb


class Product(base.MidpageProduct):
    # 每个类别的指标项结构
    def template(self):
        ret = {
            'pv':0,
            'uv':0,
            'upv':0,
            u'图片点击率':0,
            u'图片滑动率':0,
            u'电话点击率':0,
            u'地址点击率':0,
            u'评论点击率':0,
            u'团购信息点击率':0,
            u'其他团购按钮点击率':0,
            u'图片展现数':0,
            u'评论展现量':0,
            u'团购信息展现数':0,
            u'展现电话次数':0,
            u'展现地址次数':0,
            u'榜单模块点击率':0,
            u'菜品推荐模块点击率':0,
            u'相关评价tag整体点击率':0,
            u'榜单H5页的PV':0,
            u'榜单H5页的UV':0,
            u'榜单H5页的餐厅点击率':0,
            u'菜品推荐H5页的PV':0,
            u'菜品推荐H5页的UV':0,
            u'页面停留平均时间':0
        }
        return ret

    # 整体结构
    def getRetStruct(self):
        na = {}
        na['ios'] = self.template()
        na['android'] = self.template()
        na['total'] = self.template()
        mb = {}
        mb['ios'] = self.template()
        mb['android'] = self.template()
        mb['total'] = self.template()
        total = {}
        total['ios'] = self.template()
        total['android'] = self.template()
        total['total'] = self.template()
        ret = {}
        ret['NA'] = na
        ret['MB'] = mb
        ret['total'] = total
        return ret

    # 数据库查询构建函数
    def getInfo(self,collection,match,groupList):
        commandList = []
        commandList.append({'$match':match})
        for obj in groupList:
            commandList.append({'$group':obj})
        ret = collection.aggregate(commandList);
        return ret

    # 指标计算
    def indexCount(self, collection, dataset):
        ret = self.template()
        #pv
        match = {'query.cat':'dumi_meishi','query.act':'pv'}
        match.update(dataset)
        ret['pv'] = collection.find(match).count()
        #uv
        ret['uv'] = len(collection.find(match).distinct('baiduid'))
        #upv
        upvCol = collection.aggregate([
            {'$match':match},
            {'$group':{'_id':{'baiduid':"$baiduid",'meishi_id':"$query.meishi_id"},'count':{'$sum':1}}},
            {'$group':{'_id':'','count':{'$sum':1}}}
        ]);
        for obj in upvCol:
            ret['upv'] = obj['count']
        #图片点击率
        match = {'query.cat':'dumi_meishi','query.act':'b_click_focusimg'}
        match.update(dataset)
        ret[u'图片点击率'] = float(collection.find(match).count())/ret['pv'] if ret['pv'] else 0
        #图片滑动率
        match = {'query.cat':'dumi_meishi','query.act':'b_slide_focusimg'}
        match.update(dataset)
        ret[u'图片滑动率'] = float(collection.find(match).count())/ret['pv'] if ret['pv'] else 0
        #电话点击率
        match = {'query.cat':'dumi_meishi','query.act':'b_click_tel'}
        match.update(dataset)
        ret[u'电话点击率'] = float(collection.find(match).count())/ret['pv'] if ret['pv'] else 0
        #地址点击率
        match = {'query.cat':'dumi_meishi','query.act':'a_click_location'}
        match.update(dataset)
        ret[u'地址点击率'] = float(collection.find(match).count())/ret['pv'] if ret['pv'] else 0
        #评论点击率
        match = {'query.cat':'dumi_meishi','query.act':'a_click_comment'}
        match.update(dataset)
        ret[u'评论点击率'] = float(collection.find(match).count())/ret['pv'] if ret['pv'] else 0
        #团购信息点击率
        
        match = {
                    'query.cat':'dumi_meishi',
                    'query.act':'a_click_groupon_item',
                }
        match.update(dataset)
        total_click = collection.find(match).count()
        match = {
                    'query.cat':'dumi_meishi',
                    'query.act':'a_click_groupon_item',
                    '$or':[
                        {'query.xpath':'div-div2-div3-section5-div2-a-div-div(link)'},
                        {'query.xpath':'div-div2-div3-section5-div-a-div-div(link)'}
                    ]
                }
        match.update(dataset)
        tuangou_click = collection.find(match).count()
        other_click = total_click - tuangou_click
        ret[u'团购信息点击率'] = float(tuangou_click)/ret['pv'] if ret['pv'] else 0
        #其他团购按钮点击率
        ret[u'其他团购按钮点击率'] = float(other_click)/ret['pv'] if ret['pv'] else 0
        #图片展现数
        match = {'query.cat':'dumi_meishi','query.act':'pv'}
        match.update(dataset)
        show_info = collection.aggregate([
            {'$match':match},
            {'$group':{'_id':'','image_show':{'$sum':'$query.image_num'},
                'com_show':{'$sum':'$query.review_num'},'group_show':{'$sum':'$query.tuangou_num'}}}
        ]);
        for obj in show_info:
            ret[u'图片展现数'] = obj['image_show']
            ret[u'评论展现量'] = obj['com_show']
            ret[u'团购信息展现数'] = obj['group_show']
        #展现电话次数
        match = {'query.cat':'dumi_meishi','query.act':'pv','query.has_phone':'1'}
        match.update(dataset)
        ret[u'展现电话次数'] = float(collection.find(match).count())
        #展现地址次数
        match = {'query.cat':'dumi_meishi','query.act':'pv','query.has_address':'1'}
        match.update(dataset)
        ret[u'展现地址次数'] = float(collection.find(match).count())
        #榜单模块点击率
        match = {'query.cat':'dumi_meishi','query.act':'a_click_toplist'}
        match.update(dataset)
        toplist_click = collection.find(match).count()
        ret[u'榜单模块点击率'] = float(toplist_click)/ret['pv'] if ret['pv'] else 0
        #菜品推荐模块点击率
        match = {'query.cat':'dumi_meishi','query.act':'a_click_recommend_food'}
        match.update(dataset)
        recommed_food_click = collection.find(match).count()
        ret[u'菜品推荐模块点击率'] = float(recommed_food_click)/ret['pv'] if ret['pv'] else 0
        #相关评价tag整体点击率
        match = {'query.cat':'dumi_meishi','query.act':'b_click_comment_tag'}
        match.update(dataset)
        tag_click = collection.find(match).count()
        ret[u'相关评价tag整体点击率'] = float(tag_click)/ret['pv'] if ret['pv'] else 0
        #榜单H5页的PV
        match = {'query.cat':'dumi_meishi_toplist','query.act':'pv'}
        match.update(dataset)
        ret[u'榜单H5页的PV'] = collection.find(match).count()
        #榜单H5页的UV
        match = {'query.cat':'dumi_meishi_toplist','query.act':'pv'}
        match.update(dataset)
        ret[u'榜单H5页的UV'] = len(collection.find(match).distinct('baiduid'))
        #榜单H5页的餐厅点击率
        match = {'query.cat':'dumi_meishi_toplist','query.act':'a_click_rank_item'}
        match.update(dataset)
        rank_item_click = collection.find(match).count()
        ret[u'榜单H5页的餐厅点击率'] = float(rank_item_click)/ret[u'榜单H5页的PV'] if ret[u'榜单H5页的PV']  else 0
        #菜品推荐H5页的PV
        match = {'query.cat':'dumi_meishi_recommend_food','query.act':'pv'}
        match.update(dataset)
        ret[u'菜品推荐H5页的PV'] = collection.find(match).count()
        #菜品推荐H5页的UV
        match = {'query.cat':'dumi_meishi_recommend_food','query.act':'pv'}
        match.update(dataset)
        ret[u'菜品推荐H5页的UV'] = len(collection.find(match).distinct('baiduid'))
        #页面停留平均时间
        match = {'query.cat':'dumi_meishi','query.act':'stay_time'}
        match.update(dataset)
        cursor = collection.find(match,['query.duration'])
        total_num = collection.find(match).count()
        total_time = 0
        for obj in cursor:
            total_time += float(obj['query']['duration'])
        ret[u'页面停留平均时间'] = total_time/total_num if total_num else 0
        return ret

    #主函数 数据统计
    def statist(self):
        u"""
        self.log_collection可以拿到mongo中的日志集合，用于统计指标的函数
        """
        ret = self.getRetStruct()
        #连接数据库
        food_Db = midpagedb.DateLogDb()
        food = food_Db.get_collection()
        foodDb = food_Db.get_db()
        
        ret['total']['total'] = self.indexCount(food, {})
        ret['total']['ios'] = self.indexCount(food, {'os':'ios'})
        ret['total']['android'] = self.indexCount(food, {'os':'android'})
        ret['NA']['total'] = self.indexCount(food, {'client':'NA'})
        ret['NA']['ios'] = self.indexCount(food, {'client':'NA','os':'ios'})
        ret['NA']['android'] = self.indexCount(food, {'client':'NA','os':'android'})
        ret['MB']['total'] = self.indexCount(food, {'client':'MB'})
        ret['MB']['ios'] = self.indexCount(food, {'client':'MB','os':'ios'})
        ret['MB']['android'] = self.indexCount(food, {'client':'MB','os':'android'})
        
        return ret

    def save_result(self, result):
        u"""
        result为statist返回的结果，用于存储结果，可以存储到本地也可以存储如数据库
        """
        date = self.date
        output = conf.OUTPUT_DIR
        midpage_output = os.path.join(output, "midpage/food/%s" % self.date)
        if not os.path.exists(midpage_output):
            os.makedirs(midpage_output)
        operate_systems = ('total','android','ios')
        titles = ('pv','uv','upv',u'图片点击率',u'图片滑动率', u'电话点击率',u'地址点击率',u'评论点击率',u'团购信息点击率',
            u'其他团购按钮点击率',u'图片展现数',u'评论展现量',u'团购信息展现数',u'展现电话次数',u'展现地址次数',u'榜单模块点击率',
            u'菜品推荐模块点击率',u'相关评价tag整体点击率', u'榜单H5页的PV', u'榜单H5页的UV',u'榜单H5页的餐厅点击率',
            u'菜品推荐H5页的PV', u'菜品推荐H5页的UV',u'页面停留平均时间')
        file_other = os.path.join(midpage_output, 'total.txt')
        with open(file_other, 'w') as f:
            count = 100
            for operate_system in operate_systems:
                for title in titles:
                    out = u'%s\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['total'][operate_system][title])
                    f.write(out.encode('utf-8'))
                    count = count + 1

        file_na = os.path.join(midpage_output, 'NA.txt')
        with open(file_na, 'w') as f:
            count = 100
            for operate_system in operate_systems:
                for title in titles:
                    out = u'%s\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['NA'][operate_system][title])
                    f.write(out.encode('utf-8'))
                    count = count + 1

        file_mb = os.path.join(midpage_output, 'MB.txt')
        with open(file_mb, 'w') as f:
            count = 100
            for operate_system in operate_systems:
                for title in titles:
                    out = u'%s\t%s\t%s\t%s\r\n' % (count, operate_system, title, result['MB'][operate_system][title])
                    f.write(out.encode('utf-8'))
                    count = count + 1
