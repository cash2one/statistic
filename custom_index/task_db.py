# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : task_db.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
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


class CustomIndexTask(mysql_db.BaseMysqlDb):
    u"""
    对应custom index task表
    """
    table_name = 'rawdata_task'

    def __init__(self, task_id):
        super(CustomIndexTask, self).__init__()
        self._get_task_info(task_id)

    def _get_task_info(self, task_id):
        sql = "select `id`,`name`,`owner`,`sub_project_id`,`routine`,`path`, " \
              "`time_delta`,`hour`,`period`,`last_run_date`, `type`, `last_run_time` from %s where id=%s" %\
              (self.table_name, task_id)
        self.cur.execute(sql)
        line = self.cur.fetchone()
        if line is None:
            raise error.DataBaseError(u"task %s not find" % task_id)
        self.id = line[0]
        self.name = line[1]
        self.owner = line[2]
        self.sub_project_id = line[3]
        self.routine = True if line[4] else False
        self.path = line[5]
        self.time_delta = line[6]
        self.hour = line[7]
        self.period = line[8]
        self.last_run_date = line[9]
        self.task_type = line[10]
        self.last_run_time = line[11]

        if not self.period:
            self.period = {"type":"daily"}
        else:
            self.period = json.loads(self.period)

    def update_last_run_date(self, update_date, update_time="00:00"):
        str_date = time.strftime("%Y-%m-%d", time.strptime(update_date, "%Y%m%d"))
        sql = "update %s set last_run_date='%s' where id=%s and (last_run_date<'%s' or last_run_date is null)" %\
              (self.table_name, str_date, self.id, str_date)
        logging.info(sql)
        self.cur.execute(sql)
        # 增加了一个精确到分钟的更新时间。为考虑兼容性，原有天级暂时不删除
        str_time = base.date_time_str2utc(update_date, update_time)
        sql = "update %s set last_run_time='%s' where id=%s and (last_run_time<'%s' or last_run_time is null)" %\
              (self.table_name, str_time, self.id, str_time)
        logging.info(sql)
        self.cur.execute(sql)
        self.conn.commit()

    def if_run_today(self):
        if self.period["type"] == "daily":
            return True
        elif self.period["type"] == "weekly":
            today = datetime.date.today()
            if today.weekday() == self.period["weekday"]:
                return True
            else:
                return False
        raise error.Error("unknown period type")

    def if_run_now(self):
        now = datetime.datetime.now()
        # 为时效性任务
        if self.task_type == "timely":
            # 现在时间大于首次运行时间
            str_now = "%s:%s" % (now.hour, now.minute)
            if str_now > self.period["first"]:
                # 距离上次运行时间大于运行间隔
                last = base.utc_str2date_time(self.last_run_time)
                if now - last > datetime.timedelta(minutes=self.period["interval"]):
                    return True
        return False

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
