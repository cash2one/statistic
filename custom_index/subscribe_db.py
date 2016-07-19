# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : subscribe.py

Authors: xulei12@baidu.com
Date   : 2016-07-18
Comment:
"""
# 标准库
import json
import time
import logging
import datetime
# 第三方库

# 自有库
from lib import mysql_db
from lib import error
import base


class Subscribe(mysql_db.BaseMysqlDb):
    u"""
    对应subcribe表
    """
    table_name = 'rawdata_subscribe'

    def __init__(self, indicators, sub_project_id):
        super(Subscribe, self).__init__()
        self.info = []
        self._get_info(indicators, sub_project_id)

    def _get_info(self, indicators, sub_project_id):
        query = ""
        for item in indicators:
            query += "'%s'," % item
        query = "(" + query[0:-1] + ")"
        sql = "select `indicator`,`user`,`page`,`dimension` from %s where sub_project_id=%s and indicator in %s" %\
              (self.table_name, sub_project_id, query)
        logging.info(sql)
        self.cur.execute(sql)
        for line in self.cur.fetchall():
            item = {
                "indicator": line[0],
                "user": line[1],
                "page": line[2],
                "dimension": line[3],
            }
            if item["dimension"]:
                item["dimension"] = json.loads(item["dimension"])
            else:
                pass
            self.info.append(item)

    @classmethod
    def get_routine_tasks_by_hour(cls, hour):
        conn, cur = cls._get_connect()
        sql = "select `id` from %s where routine=1 and hour=%s and mark_del=0" % (cls.table_name, hour)
        cur.execute(sql)
        lines = cur.fetchall()
        lines = [line[0] for line in lines]
        return lines

    @classmethod
    def get_unroutine_tasks(cls):
        conn, cur = cls._get_connect()
        sql = "select `id` from %s where routine=0 and `type`='index' and mark_del=0" % cls.table_name
        cur.execute(sql)
        lines = cur.fetchall()
        lines = [line[0] for line in lines]
        return lines

    @classmethod
    def get_timely_tasks(cls):
        """
        获取所有时效性指标任务的id
        xulei12@baidu.com
        2016.07.06
        :return:
        """
        conn, cur = cls._get_connect()
        sql = "select `id` from %s where type='timely' and mark_del=0" % cls.table_name
        cur.execute(sql)
        lines = cur.fetchall()
        lines = [line[0] for line in lines]
        return lines

if __name__ == "__main__":
    print "xx"