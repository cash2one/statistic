# coding=utf-8
import pymongo

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

    def clear(self):
        self.collection.remove()

    def distinct_count(self, field, cons):
        count = 0
        obj = []
        obj.append({'$match': cons})

        dist = {}
        if len(field) > 0:
            for i in field:
                dist[i] = '$%s' % i
        obj.append({'$group': {'_id': dist, 'count':{'$sum':1}}})
        obj.append({'$group':{'_id':'', 'count':{'$sum':1}}})
        cursor = self.collection.aggregate(obj, allowDiskUse=True)
        for item in cursor:
            count = item['count']
        return count
