# coding=utf-8
import re
import MySQLdb
import datetime
from conf import conf

def get_date(mode):
    today = datetime.datetime.today()
    year = today.year
    month = today.month
    if mode == "last":
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    elif mode == "next":
        month += 1
        if month == 13:
            month = 1
            year += 1
    return "%s%02d"%(year, month)


def openDb():
    try:
        con = MySQLdb.connect(host=conf.DB_HOST,
            user=conf.DB_USER,  passwd=conf.DB_PASSWORD,
            db=conf.DB_NAME, port=conf.DB_PORT,
            charset='utf8')
        return con
    except:
        g.logger.ex()
        if con:
            con.close()

def closeDb(con):
    if con:
        con.close()

def get_init_sql():
    sql_list = []
    #spo
    spo_product = "CREATE TABLE IF NOT EXISTS `spo_product`(\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `name` varchar(255) NOT NULL,\
        `srcids` text NOT NULL,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        PRIMARY KEY (`id`),\
        INDEX `name_index` (`name`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    spo_srcid_stat = "CREATE TABLE IF NOT EXISTS `spo_srcid_stat`(\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `srcid` varchar(32) NOT NULL,\
        `stat` varchar(255) NOT NULL,\
        `value` double(32, 10) NOT NULL,\
        `date` date NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `srcid_index` (`srcid`),\
        INDEX `stat_index` (`stat`),\
        INDEX `date_index` (`date`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    spo_query_stat = "CREATE TABLE IF NOT EXISTS `spo_query_stat`(\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `srcid` varchar(32) NOT NULL,\
        `query` varchar(4096) NOT NULL,\
        `pv` bigint(20) NOT NULL,\
        `date` date NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `srcid_index` (`srcid`),\
        INDEX `date_index` (`date`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	#midpage
    midpage_product = "CREATE TABLE IF NOT EXISTS `midpage_product`(\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `name` varchar(255) NOT NULL,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `source` varchar(255) NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `name_index` (`name`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    midpage_stat = "CREATE TABLE IF NOT EXISTS `midpage_stat` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `product_id` int(11) unsigned NOT NULL,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `stat` varchar(255) NOT NULL,\
        `value` double(32, 10) NOT NULL,\
        `date` date NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `product_id_index` (`product_id`),\
        INDEX `stat_index` (`stat`),\
        INDEX `date_index` (`date`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    midpage_position_stat = "CREATE TABLE IF NOT EXISTS `midpage_position_stat` (\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `product_id` int(11) unsigned NOT NULL,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `position` int(11) NOT NULL,\
        `click_num` bigint(20) NOT NULL,\
        `show_num` bigint(20) NOT NULL,\
        `click_rate` double(32, 10) NOT NULL,\
        `date` date NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `product_id_index` (`product_id`),\
        INDEX `date_index` (`date`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    sql_list = [spo_product, spo_srcid_stat, spo_query_stat, midpage_product, 
    	midpage_stat, midpage_position_stat]
    return sql_list


def get_month_sql(date):
    sql_list = []
    midpage_url_stat = "CREATE TABLE IF NOT EXISTS `midpage_url_stat_%s`(\
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
        `product_id` int(11) unsigned NOT NULL,\
        `side` varchar(16) NOT NULL DEFAULT '',\
        `url` varchar(255) NOT NULL,\
        `click_num` bigint(20) NOT NULL,\
        `show_num` bigint(20) NOT NULL,\
        `click_rate` double(32, 10) NOT NULL,\
        `date` date NOT NULL,\
        PRIMARY KEY (`id`),\
        INDEX `product_id_index` (`product_id`),\
        INDEX `date_index` (`date`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8" % date
    sql_list.append(midpage_url_stat)
    return sql_list


def create(mode):
    sql_list = []
    date = get_date(mode)
    if mode == "init":
        sql_list.extend(get_init_sql())
    sql_list.extend(get_month_sql(date))
    con = openDb()
    
    try:
        cur = con.cursor()
        for sql in sql_list:
            cur.execute(sql)
        con.commit()
    except Exception,e:
        g.logger.ex()
        con.rollback()
    closeDb(con)
