# coding=utf-8
import pymongo

from conf import conf


class BaseMongoLogDb(object):
    DB_NAME = conf.MONGO_DB
    COLLECTION_NAME = ""

    def __init__(self):
        self.conn = self._get_connect()
        self.db = self.conn[self.DB_NAME]
        self.collection = self.db[self.COLLECTION_NAME]

    @staticmethod
    def _get_connect():
        conn = pymongo.MongoClient(conf.MONGO_HOST, conf.MONGO_PORT, connect=False)
        return conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    def _get_db(self):
        return self.db

    def _get_collection(self):
        return self.collection

    def insert(self, data):
        if type(data) == list:
            self.collection.insert_many(data)
        else:
            self.collection.insert_one(data)

    def update(self, query, data):
            self.collection.replace_one(query, data, upsert=True)

    def _clear(self):
        self.collection.remove()

    def __getattr__(self, key):
        attr = getattr(self.collection, key)
        if attr is None:
            raise AttributeError(u"uknown attribute %s" % key)
        return attr


class BaseMongoFontDb(BaseMongoLogDb):
    DB_NAME = conf.MONGO_FONT_DB
