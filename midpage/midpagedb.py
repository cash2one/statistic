#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : midpagedb.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-20
Comment:
"""
# 标准库
import pymongo
# 第三方库

# 自有库
from conf import conf


class DateLogDb(object):
    date = ''

    def __init__(self):
        self.collection_name = 'datelog_%s' % self.date
        self.conn = pymongo.MongoClient(conf.MONGO_HOST, conf.MONGO_PORT)
        self.db = self.conn[conf.MONGO_DB]
        self.collection = self.db[self.collection_name]

    @classmethod
    def set_date(cls, date):
        cls.date = date

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    def get_db(self):
        return self.db

    def get_collection(self):
        return self.collection

    def insert_log(self, logs):
        if type(logs) == list:
            self.collection.insert_many(logs)
        else:
            self.collection.insert_one(logs)

    def create(self):
        self.collection.ensure_index('source')
        self.collection.ensure_index('url')
        self.collection.ensure_index('query.cat')

    def clear(self, sources=None):
        if sources:
            for source in sources:
                self.collection.remove({'source':source})
        else:
            self.collection.drop()
            self.create()

    def distinct_count(self, field, cons):
        count = 0
        obj = [{'$match': cons}]

        dist = {}
        if len(field) > 0:
            for i in field:
                dist[i] = '$%s' % i
        obj.append({'$group': {'_id': dist, 'count': {'$sum': 1}}})
        obj.append({'$group': {'_id': '', 'count': {'$sum': 1}}})
        cursor = self.collection.aggregate(obj, allowDiskUse=True)
        for item in cursor:
            count = item['count']
        return count


class FrontMongoDb(object):
    """
    用于推送数据到kgdc前台展现数据库，
    同时推到表original_data 和 daily_summary_data
    original_data在前台用于原始推送数据
    daily_summary_data，对于周级数据，会填满每一天的然后推送到该表（这些操作在前台程序处理），对于天级数据，直接推送。
    此处都按照天级推送
    xulei12@baidu.com 2016.06.20
    """
    def __init__(self):
        self.conn = pymongo.MongoClient(conf.MONGO_HOST, conf.MONGO_PORT)
        self.db = self.conn[conf.MONGO_FONT_DB]
        self.collection_origin = self.db["original_data"]
        self.collection_summary = self.db["daily_summary_data"]

    def __del__(self):
        self.close()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def insert(self, data):
        if type(data) == list:
            self.collection_origin.insert_many(data)
            self.collection_summary.insert_many(data)
        else:
            self.collection_origin.insert_one(data)
            self.collection_summary.insert_one(data)
