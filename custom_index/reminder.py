#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：
订阅提醒相关功能接口

File   : reminder.py

Authors: xulei12@baidu.com
Date   : 2016/7/18
Comment: 
"""
# 系统库
import json
import copy
import datetime
import logging
# 第三方库

# 自有库
import base
import subscribe_db
from lib import mysql_db


class Reminder(object):
    def __init__(self, task, date, mongo_db, indicators):
        self.task = task
        self.date = date
        self.db = mongo_db
        self.indicators = indicators
        self.indicator_id2name_map = {}
        self.indicator_name2id_map = {}
        self.subscribe = subscribe_db.Subscribe(self.indicators, task.sub_project_id)
        self._get_indicators()

    def _get_indicators(self):
        db = mysql_db.BaseMysqlDb()
        sql = "select `id`,`name` from rawdata_indicator where sub_project_id=%s" % self.task.sub_project_id
        db.cur.execute(sql)
        for item in db.cur.fetchall():
            self.indicator_name2id_map[item[0]] = item[1]
            self.indicator_id2name_map[item[1]] = item[0]

    def arrange_by_indicator_and_page(self, info):
        # indicator + page 唯一确定一个订阅信息。把拉平的数据,把用户信息整理合并
        # page与dimension一一对应。
        ret = []
        for item in info:
            query = {
                "indicator": item["indicator"],
                "page": item["page"]
            }
            query_ret = base.json_list_find(ret, query)
            # 已经有了，则合并
            if query_ret:
                query_ret["user"].append(item["user"])
            # 否则user字段转换成list
            else:
                item["user"] = [item["user"]]
                ret.append(copy.deepcopy(item))
        return ret

    def calculate_value(self, info):
        # 根据整理好的以 indicator + page 为维度的信息，计算发送的订阅信息
        # 发送的订阅信息如下
        date = datetime.datetime.strptime(self.date, "%Y%m%d")
        yesterday = date - datetime.timedelta(days=1)
        last_week_date = date - datetime.timedelta(days=6)
        # 统一转换成字符串
        date = self.date
        yesterday = yesterday.strftime("%Y%m%d")
        last_week_date = last_week_date.strftime("%Y%m%d")
        for item in info:
            query = {
                "@index": item["indicator"],
                "@create": {"$gte": last_week_date, "$lte": date},
                "@subProject": self.task.sub_project_id
            }
            query.update(item["dimension"])
            logging.info(query)
            query_ret = []
            for one_query in self.db.find(query):
                del one_query["_id"]
                query_ret.append(one_query)
            email_msg = {}
            if self.task.period["type"] == "weekly":
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": date})
                email_msg["this_value"] = (find_ret["@value"] if "@value" in find_ret else "-")
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": last_week_date})
                email_msg["last_value"] = (find_ret["@value"] if "@value" in find_ret else "-")
                value = self.get_diff_rate(email_msg["this_value"], email_msg["last_value"])
                email_msg["diff_rate"] = (value if value else "-")
                email_msg["week_diff_rate"] =
            elif self.task.period["type"] == "daily":
                pass
            else:
                pass

    def get_diff_rate(self, new_data, old_data):
        try:
            new_data = float(new_data)
            old_data = float(old_data)
        except Exception as e:
            return ""
        if not new_data or not old_data:
            return ""
        return (new_data - old_data) / old_data

    def send_remind_email(self, data):
        """
        根据data的内容，发送邮件，并记录日志。

        :param data: 发送的订阅数据，格式如下：
        {
            "xulei12":      # 要发送的用户
                {
                    "pv":   # 指标名
                        {
                            "this_value": 123,      # 今日/本周值
                            "last_value": 120,      # 昨日/上周值
                            "diff_rate": 0.02,      # 日/周环比
                            "week_diff_rate": 0.01, # 周同比
                            "week_avg": 122,        # 周均
                            "link": "kgdc.baidu.com/perform/1" # 链接
                        },
                    "uv": {}
                },
            "liuyangang": {}
        }
        :return: 发送成功的邮件数量
        """
        pass

def test():
    import task_db
    import data_db
    task_id = 46
    date = "20160717"
    task = task_db.CustomIndexTask(task_id)
    original_data = data_db.OriginalData()
    indicators = ["pv", "session_num"]
    remind = Reminder(task=task, date=date, mongo_db=original_data, indicators=indicators)
    print json.dumps(remind.indicator_id2name_map, ensure_ascii=False)
    print json.dumps(remind.indicator_name2id_map, ensure_ascii=False)

    print len(remind.subscribe.info)
    info = remind.arrange_by_indicator_and_page(remind.subscribe.info)
    print len(info)
    for item in info:
        print json.dumps(item, ensure_ascii=False)
    remind.calculate_value(info)
