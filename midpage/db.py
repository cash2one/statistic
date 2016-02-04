# -*- coding: utf-8 -*-
import os
import json
import MySQLdb
import types

from conf import conf
from lib import tools

class DataBase(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host=conf.DB_HOST,
            user=conf.DB_USER,  passwd=conf.DB_PASSWORD,
            db=conf.DB_NAME, port=conf.DB_PORT,
            charset='utf8')
        self.cur = self.conn.cursor()
        
    def close(self):
        self.cur.close()
        self.conn.close()

class SelectDataBase(DataBase):
    def get_midpage_product(self):
        sql = "select * from midpage_product"
        self.cur.execute(sql)

        return self.cur.fetchall()


class SaveDataBase(DataBase):
    def __init__(self):
        super(SaveDataBase, self).__init__()

    def escape_string(self, unicode_string):
        utf_8_string = unicode_string.encode("utf-8")
        utf_8_string = MySQLdb.escape_string(utf_8_string)
        return utf_8_string.decode("utf-8")
   
    def clear_midpage_daily_summary(self, date, index):
        sql = "delete from midpage_daily_summary where `date`='%s'" %(date)
        if len(index) > 0:
            index_str = []
            for key in index:
                index_str.append('"'+ key +'"')
            sql += ' and `index` in (' + ','.join(index_str) + ')'
        self.cur.execute(sql)
        self.conn.commit()

    def save_spo_index_info(self, rows):
        u"""
        [
            (side, ...)
        ]
        """
        sql = "insert into midpage_daily_summary (`side`,`product`, `pid`,`index`, `value`, `last_modify_date`, `date`) \
            values %s"

        values = []
        for value in rows:
            s = "('%s','%s', '%s','%s','%s','%s','%s')" % (value['side'], value['product'], value['pid'],\
                 value['index'], value['value'], value['last_modify_date'], value['date'])
            values.append(s)
        
        values = ",".join(values)
        sql = sql % (values, )
        self.cur.execute(sql)
        self.conn.commit()

