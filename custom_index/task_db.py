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
        self.conn.commit()

    def update_last_run_time(self, update_date, update_time="00:00"):
        """
        此处的date格式为2016-07-06
        :param update_date:
        :param update_time:
        :return:
        """
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
        """
        时效性任务判断
        :return: -1 大于00:00小于首次运行时间
                  0  不运行
                  1  大于首次运行时间，运行
        """
        # 为时效性任务
        if self.task_type != "timely":
            return 0

        # 统一换算为分钟数
        ts_now = int(time.time()/60)
        # added by xulei12@baidu.com 2016.7.15 当指标延迟导入，可能每天最后的指标无法导入
        str_now = time.strftime("%H:%M")
        # added ended
        # 计算首次运行的ts分钟
        today = datetime.date.today()
        first_time = str(today) + " " + self.period["first"]
        first_time = time.strptime(first_time, "%Y-%m-%d %H:%M")
        first_ts = int(time.mktime(first_time)/60)

        # added by xulei12@baidu.com 2016.7.15 当指标延迟导入，可能每天最后的指标无法导入
        # 不能直接使用ts比较，因为实际上需要的是今天，而且小于首次运行时间
        if str_now < self.period["first"]:
            return -1
        # added ended
        # 现在时间大于首次运行时间
        if ts_now >= first_ts:
            if (ts_now-first_ts) % (self.period["interval"]) == 0:
                return 1
        return 0

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
