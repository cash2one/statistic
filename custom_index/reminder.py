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
        self.email_data = {}
        self.indicator_source2name_map = {}
        self.indicator_name2source_map = {}
        self.subscribe = subscribe_db.Subscribe(self.indicators, task.sub_project_id)
        self._get_indicators()
        self.info = self.subscribe.info

    def _get_indicators(self):
        db = mysql_db.BaseMysqlDb()
        sql = "select `source_name`,`name` from rawdata_indicator where sub_project_id=%s" % self.task.sub_project_id
        db.cur.execute(sql)
        for item in db.cur.fetchall():
            self.indicator_source2name_map[item[0]] = item[1]
            self.indicator_name2source_map[item[1]] = item[0]

    def arrange_by_indicator_and_page(self):
        """
        indicator + page 唯一确定一个订阅信息。把拉平的数据,把用户信息整理合并成list
        page与dimension一一对应。
        入口数据格式为
        [{
            'indicator': 'pv',
            'dimension': {'product': 'all_shoubai', 'os': '总体'},
            'user': 'xulei12'
            , 'page': 6
        }, {
            'indicator': 'session_num',
            'dimension': {'product': 'all_shoubai', 'os': '总体'},
            'user': 'xulei12',
            'page': 6
        }, {
            'indicator': 'session_num',
            'dimension': {'product': 'all_app', 'os': '总体'},
            'user': 'liuyangang',
            'page': 6
        }, {
            'indicator': 'session_num',
            'dimension': {'product': 'emotion_all', 'os': '总体'},
            'user': 'xulei12',
            'page': 15
        }]
        :return:
        出口的数据格式为
        [{
            "indicator": "pv",
            "page": 6,
            "dimension": {"product": "all_shoubai", "os": "总体"},
            "name": "pv",
            "user": ["xulei12"]
        }, {
            "indicator": "session_num",
            "page": 6,
            "dimension": {"product": "all_shoubai", "os": "总体"},
            "name": "Session数量",
            "user": ["xulei12", "liuyangang"]
        }, {
            "indicator": "session_num",
            "page": 15,
            "dimension": {"product": "emotion_all", "os": "安卓"},
            "name": "Session数量",
            "user": ["xulei12"]
        }]
        """
        ret = []
        info = self.info
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
                item["name"] = self.indicator_source2name_map[item["indicator"]]
                ret.append(copy.deepcopy(item))
        self.info = ret

    def calculate_value(self):
        """
        根据整理好的以 indicator + page 为维度的信息，计算发送的订阅信息
        :return:
        入口为接口arrange_by_indicator_and_page()的数据。出口数据格式为
        [{
            "indicator": "pv",
            "name": "pv",
            "page": 6,
            "dimension": {"product": "all_shoubai", "os": "总体"},
            "user": ["xulei12"],
            "email_msg": {
                "indicator": "pv",
                "this_value": "104291.0",
                "last_value": "104053.0",
                "diff_rate": "0.0023",
                "week_diff_rate": "0.1616",
                "week_avg": "99983.5714",
                "page": 6,
                "page_name": "",
                "link": "http://kgdc.baidu.com/perform/6"
            },
        }, {
            "indicator": "session_num",
            "name": "Session数量",
            "page": 6,
            "dimension": {"product": "all_shoubai", "os": "总体"},
            "user": ["xulei12", "liuyangang"],
            "email_msg": {
                "indicator": "session_num",
                "this_value": "20083.0",
                "last_value": "20225.0",
                "diff_rate": "-0.007",
                "week_diff_rate": "0.008",
                "week_avg": "20639.7143",
                "page": 6,
                "page_name": "",
                "link": "http://kgdc.baidu.com/perform/6"
            },
        }, {
            "indicator": "session_num",
            "name": "Session数量",
            "page": 15,
            "dimension": {"product": "emotion_all", "os": "安卓"},
            "user": ["xulei12"],
            "email_msg": {
                "indicator": "session_num",
                "this_value": "-",
                "last_value": "-",
                "diff_rate": "-",
                "week_diff_rate": "-",
                "week_avg": "-",
                "page": 15,
                "page_name": "",
                "link": "http://kgdc.baidu.com/perform/15"
            },
        }]
        """
        info = self.info
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
            logging.debug("query is :\n%s" % json.dumps(query, ensure_ascii=False))
            query_ret = []
            for one_query in self.db.find(query):
                del one_query["_id"]
                query_ret.append(one_query)
            email_msg = {
                "indicator": item["indicator"],
                "page": item["page"],
                "page_name": "",
                "link": "http://kgdc.baidu.com/perform/%s" % item["page"]
            }
            if self.task.period["type"] == "weekly":
                # 获取今天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": date})
                email_msg["this_value"] = find_ret.get("@value", "-")
                # 获取上周同一天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": last_week_date})
                email_msg["last_value"] = find_ret.get("@value", "-")
                # 获取周同比
                email_msg["diff_rate"] = base.get_diff_rate(email_msg["this_value"], email_msg["last_value"], 4)
                # 其他为空
                email_msg["week_diff_rate"] = "-"
                email_msg["week_avg"] = "-"
            elif self.task.period["type"] == "daily":
                # 获取今天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": date})
                email_msg["this_value"] = find_ret.get("@value", "-")
                # 获取昨天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": yesterday})
                email_msg["last_value"] = find_ret.get("@value", "-")
                # 获取天同比
                email_msg["diff_rate"] = base.get_diff_rate(email_msg["this_value"], email_msg["last_value"], 4)
                # 获取周环比
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": last_week_date})
                last_week_value = find_ret.get("@value", "-")
                email_msg["week_diff_rate"] = base.get_diff_rate(email_msg["this_value"], last_week_value, 4)
                # 计算周均值
                total = base.json_list_sum_by(query_ret, "@value")
                if not total or len(query_ret) == 0:
                    email_msg["week_avg"] = "-"
                else:
                    email_msg["week_avg"] = str(round(total/len(query_ret), 4))
            else:
                pass
            item["email_msg"] = email_msg
            logging.debug("info is: \n%s" % json.dumps(item, ensure_ascii=False))

    def arrange_by_user(self):
        """
        将calculate_value()出口的数据整理成send_remind_email()需要的格式.
        具体数据样例见send_remind_email注释
        :param self:
        :return:
        """
        user_list = []
        data = {}
        info = self.info
        for item in info:
            user_list = list(set(user_list + item["user"]))
        logging.debug("all user is :\n%s" % user_list)

        for user in user_list:
            user_data = []
            for item in info:
                if user in item["user"]:
                    user_data.append(item["email_msg"])
            data.update({user: user_data})
        self.email_data = data
        logging.debug("data to send function is:\n%s" % json.dumps(data, ensure_ascii=False))

    def send_remind_email(self):
        """
        根据data的内容，发送邮件，并记录日志。

        :param data: 发送的订阅数据，格式如下：
        {
            "xulei12":      # 要发送的用户
                [
                    {
                        "indicator": "pv":   # 指标名
                        "this_value": "123",      # 今日/本周值
                        "last_value": "120",      # 昨日/上周值
                        "diff_rate": "0.02",      # 日/周环比
                        "week_diff_rate": "0.01", # 周同比
                        "week_avg": "122",        # 周均
                        "page": 9,              # 页面id
                        "page_name": "度秘提醒-总体概况"   # 页面名字
                        "link": "kgdc.baidu.com/perform/1" # 链接
                    },
                    {
                        "indicator": "uv",
                        ……
                    }
                ],
            "liuyangang": [],
            ……
        }
        :return: 发送成功的邮件数量
        """
        pass

    def run(self):
        self.arrange_by_indicator_and_page()
        self.calculate_value()
        self.arrange_by_user()
        self.send_remind_email()

def test():
    import task_db
    import data_db
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    task_id = 46
    date = "20160717"
    task = task_db.CustomIndexTask(task_id)
    original_data = data_db.OriginalData()
    indicators = ["pv", "session_num"]
    remind = Reminder(task=task, date=date, mongo_db=original_data, indicators=indicators)
    logging.debug(json.dumps(remind.indicator_source2name_map, ensure_ascii=False))
    logging.debug(json.dumps(remind.indicator_name2source_map, ensure_ascii=False))

    remind.arrange_by_indicator_and_page()
    remind.calculate_value()
    remind.arrange_by_user()
