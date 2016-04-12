# coding=utf-8
import MySQLdb

from conf import conf

class BaseMysqlDb(object):
    def __init__(self):
        self._connect()

    def _connect(self):
        conn, cur = self._get_connect()
        self.conn = conn
        self.cur = cur
        
    def close(self):
        if self.cur:
            self.cur.close()
            self.cur = None
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    @staticmethod
    def _get_connect():
        conn = MySQLdb.connect(host=conf.DB_HOST,
            user=conf.DB_USER,  passwd=conf.DB_PASSWORD,
            db=conf.DB_NAME, port=conf.DB_PORT,
            charset='utf8')
        cur = conn.cursor()
        return conn, cur

    @staticmethod
    def _close_connect(conn, cur):
        cur.close()
        conn.close()
