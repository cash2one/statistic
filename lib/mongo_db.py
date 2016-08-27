# coding=utf-8
import pymongo

from conf import conf


class BaseMongoLogDb(object):
    DB_HOST = conf.MONGO_HOST
    DB_PORT = conf.MONGO_PORT
    DB_NAME = conf.MONGO_DB
    DB_REPL = conf.MONGO_REPL

    COLLECTION_NAME = ""

    def __init__(self):
        self.conn = self._get_connect()
        self.db = self.conn[self.DB_NAME]
        self.collection = self.db[self.COLLECTION_NAME]

    def _get_connect(self):
        conn = pymongo.MongoClient(self.DB_HOST, self.DB_PORT, connect=False)
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

    def update(self, query, data, upsert=False):
            self.collection.replace_one(query, data, upsert=upsert)

    def _clear(self):
        self.collection.remove()

    def __getattr__(self, key):
        attr = getattr(self.collection, key)
        if attr is None:
            raise AttributeError(u"uknown attribute %s" % key)
        return attr


class BaseMongoFontDb(BaseMongoLogDb):
    DB_HOST = conf.MONGO_FRONT_HOST
    DB_PORT = conf.MONGO_FRONT_PORT
    DB_NAME = conf.MONGO_FRONT_DB
