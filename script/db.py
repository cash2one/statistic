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

    def get_midpage_product(self, id):
        sql = "select `name`,`side`,`source` from midpage_category where `id`=%s" % id
        self.cur.execute(sql)
        product = self.cur.fetchone()
        if product is not None:
            product = {
                "name": product[0],
                "side": product[1],
                "source": product[2],
            }
        return product


class SelectDataBase(DataBase):

    def get_spo_product(self):
        sql = "select * from spo_product"
        self.cur.execute(sql)

        return self.cur.fetchall()

    def get_spo_srcid_stat(self, index, side, date, srcids = None):
        sql = "select sum(value) as total from spo_srcid_stat where stat='%s' and side='%s' and date='%s'" % (index, side, date)
        if srcids:
            sql += " and srcid in (%s) " % (",".join(srcids))
        self.cur.execute(sql)

        return self.cur.fetchall()
        

class SaveDataBase(DataBase):
    def __init__(self, date, side=0):
        super(SaveDataBase, self).__init__()
        self.date = date
        self.side = side
        self.month = date[:-2]

    def escape_string(self, unicode_string):
        utf_8_string = unicode_string.encode("utf-8")
        utf_8_string = MySQLdb.escape_string(utf_8_string)
        return utf_8_string.decode("utf-8")

    def clear_spo_srcid_stat(self):
        sql = "delete from spo_srcid_stat where `side`='%s' and `date`=%s" %(self.side, self.date)
        self.cur.execute(sql)
        self.conn.commit()
    
    def clear_spo_daily_summary(self, date, index):
        sql = "delete from spo_daily_summary where `date`='%s'" %(date)
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
        sql = "insert into spo_daily_summary (`side`,`product`, `pid`,`index`, `value`, `last_modify_date`, `date`) \
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

    def save_spo_srcid_stat(self, stat, rows):
        u"""rows:
        [
            (srcid, value),
            ...
        ]
            
        """
        for data in tools.iter_list(rows): 
            sql = "insert into spo_srcid_stat (`side`,`srcid`,`stat`,`value`,`date`) \
            values %s"
            values = []
            for srcid, value in data:
                s = "('%s','%s','%s','%s','%s')" % (self.side, srcid, stat, value, self.date)
                values.append(s)
            values = ",".join(values)
            sql = sql % (values, )
            self.cur.execute(sql)
            self.conn.commit()

    def clear_spo_query_stat(self):
        sql = "delete from spo_query_stat where `side`='%s' and `date`=%s" %(self.side, self.date)
        self.cur.execute(sql)
        self.conn.commit()

    def save_spo_query_stat(self, rows):
        u"""rows:
        [
            [srcid, query, pv],
            ...
        ]
        """
        for data in tools.iter_list(rows): 
            sql = "insert into spo_query_stat (`side`,`srcid`,`query`,`pv`,`date`) values %s"
            values = []
            for row in data:
                s = "('%s','%s','%s','%s','%s')" % (self.side, row[0], self.escape_string(row[1]),\
                    row[2], self.date)
                values.append(s)
            values = ",".join(values)
            sql = sql % (values, )
            self.cur.execute(sql)
            self.conn.commit()

    def clear_midpage_stat(self, product_id):
        sql = "delete from midpage_stat where `product_id`=%s and `side`=%s and `date`=%s" %\
            (product_id, self.side, self.date)
        self.cur.execute(sql)
        self.conn.commit()

    def save_midpage_stat(self, product_id, rows):
        u"""rows:
        [
            (stat, value),
            ...
        ]
            
        """
        for data in tools.iter_list(rows): 
            sql = "insert into midpage_stat (`product_id`,`side`,`stat`,`value`,`date`) \
            values %s"
            values = []
            for stat, value in data:
                s = "('%s','%s','%s','%s','%s')" % (product_id, self.side, stat, value, self.date)
                values.append(s)
            values = ",".join(values)
            sql = sql % (values, )
            self.cur.execute(sql)
            self.conn.commit()
    
    def clear_midpage_position_stat(self, product_id):
        sql = "delete from midpage_position_stat where `product_id`=%s and `side`=%s and `date`=%s" %\
            (product_id, self.side, self.date)
        self.cur.execute(sql)
        self.conn.commit()

    def save_midpage_position_stat(self, product_id, rows):
        u"""rows:
        [
            (position, click, show, rate),
            ...
        ]
            
        """
        for data in tools.iter_list(rows): 
            sql = "insert into midpage_position_stat (`product_id`, `side`, `position`, `click_num`,\
                `show_num`, `click_rate`, `date`) values %s"
            values = []
            for line in data:
                s = "('%s','%s','%s','%s','%s','%s','%s')" % (product_id, self.side, line[0],\
                    line[1], line[2], line[3], self.date)
                values.append(s)
            values = ",".join(values)
            sql = sql % (values, )
            self.cur.execute(sql)
            self.conn.commit()

    def clear_midpage_url_stat(self, product_id):
        sql = "delete from midpage_url_stat_%s where `product_id`=%s and `side`=%s and `date`=%s" %\
            (self.month, product_id, self.side, self.date)
        self.cur.execute(sql)
        self.conn.commit()

    def save_midpage_url_stat(self, product_id, rows):
        u"""rows:
        [
            (url, click, show, rate),
            ...
        ]
            
        """
        for data in tools.iter_list(rows): 
            sql = "insert into midpage_url_stat_%s (`product_id`, `side`, `url`, `click_num`,\
                `show_num`, `click_rate`, `date`) values %s"
            values = []
            for line in data:
                s = "('%s','%s','%s','%s','%s','%s','%s')" % (product_id, self.side, line[0],\
                    line[1], line[2], line[3], self.date)
                values.append(s)
            values = ",".join(values)
            sql = sql % (self.month, values)
            self.cur.execute(sql)
            self.conn.commit()


