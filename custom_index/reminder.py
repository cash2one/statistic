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
from lib import tools

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


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
        # indicator + page 唯一确定一个订阅信息。把拉平的数据,把用户信息整理合并
        # page与dimension一一对应。
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
        # 根据整理好的以 indicator + page 为维度的信息，计算发送的订阅信息
        # 发送的订阅信息如下
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
            email_msg = {}
            if self.task.period["type"] == "weekly":
                # 获取今天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": date})
                email_msg["this_value"] = find_ret.get("@value", "-")
                # 获取上周同一天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": last_week_date})
                email_msg["last_value"] = find_ret.get("@value", "-")
                # 获取周同比
                email_msg["diff_rate"] = self.get_diff_rate(email_msg["this_value"], email_msg["last_value"], 4)
                # 其他为空
                email_msg["week_diff_rate"] = "-"
                email_msg["week_avg"] = "-"
                email_msg["link"] = "http://kgdc.baidu.com/perform/%s" % item["page"]
            elif self.task.period["type"] == "daily":
                # 获取今天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": date})
                email_msg["this_value"] = find_ret.get("@value", "-")
                # 获取昨天
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": yesterday})
                email_msg["last_value"] = find_ret.get("@value", "-")
                # 获取天同比
                email_msg["diff_rate"] = self.get_diff_rate(email_msg["this_value"], email_msg["last_value"], 4)
                # 获取周环比
                find_ret = base.json_list_find(query_ret, {"@index": item["indicator"], "@create": last_week_date})
                last_week_value = find_ret.get("@value", "-")
                email_msg["week_diff_rate"] = self.get_diff_rate(email_msg["this_value"], last_week_value, 4)
                # 计算周均值
                total = base.json_list_sum_by(query_ret, "@value")
                if not total or len(query_ret) == 0:
                    email_msg["week_avg"] = "-"
                else:
                    email_msg["week_avg"] = round(total/len(query_ret), 4)
                email_msg["link"] = "http://kgdc.baidu.com/perform/%s" % item["page"]
            else:
                pass
            item["email_msg"] = email_msg
            logging.debug("info is: \n%s" % json.dumps(item, ensure_ascii=False))

    def arrange_by_user(self):
        user_list = []
        data = []
        info = self.info
        for item in info:
            user_list = list(set(user_list + item["user"]))
        logging.debug("all user is :\n%s" % user_list)

        for user in user_list:
            user_data = {}
            for item in info:
                if user in item["user"]:
                    user_data[item["name"]] = item["email_msg"]
            data.append({user: user_data})
        logging.debug("data to send function is:\n%s" % json.dumps(data, ensure_ascii=False))
        self.data = data

    def get_diff_rate(self, new_data, old_data, ndigits=None):
        try:
            new_data = float(new_data)
            old_data = float(old_data)
        except Exception as e:
            return "-"
        if not new_data or not old_data:
            return "-"
        ret = (new_data - old_data) / old_data
        if ndigits:
            ret = round(ret, ndigits)
        self.email_data = ret

    def get_body(self):
        """

        :return:
        """
        html_doc = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">'
        html_doc += '<html xmlns="http://www.w3.org/1999/xhtml">'
        html_doc += '<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /><title>KGDC订阅通知</title><meta name="viewport" content="width=device-width, initial-scale=1.0"/></head>'
        html_doc += '<body style="margin: 0; padding: 0;">'

        table = '<table border="0" cellpadding="0" cellspacing="0" width="100%">'
        table += ('<tr><td><table align="center" border="1" cellpadding="0" cellspacing="0" width="600" style="border-collapse: collapse;">'
                '<tr>'
                    '<th width="80">指标</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                    '<th width="80">监控名</th>'
                '</tr>'
        )
            {% if monitor.desc %}
            <tr>
                <th>监控描述</th>
                <td colspan="3" style="word-break: break-all;">{{ monitor.desc }}</td>
            </tr>
            {% endif %}
            <tr>
                <th>KG模块</th>
                <td>{{ monitor.module.name }}</td>
                <th>监控工具</th>
                <td>{{ monitor.monitor_tool.name }}</td>
            </tr>
            <tr>
                <th>报警ID</th>
                <td>{{ monitor_alert.id }}</td>
                <th>报警名</th>
                <td>{{ monitor_alert.name }}</td>
            </tr>
            {% if monitor_alert.desc %}
            <tr>
                <th>报警描述</th>
                <td colspan="3" style="word-break: break-all;">{{ monitor_alert.desc }}</td>
            </tr>
            {% endif %}
            <tr>
                <th>报警等级</th>
                <td>{{ level_name }}</td>
                {% ifequal continue_time '' %}
                <th></th>
                <td></td>
                {% else %}
                <th><font color="red">持续时间</font></th>
                <td><font color="red">{{ continue_time }}分钟</font></td>
                {% endifequal %}
            </tr>
            <tr>
                <th>监控负责人</th>
                <td colspan="3">{{ monitor.owner }}</td>
            </tr>
            <tr>
                <th>报警时间</th>
                <td>{{ check_time }}</td>
                <th>检测时间</th>
                <td>{{ begin_time }}</td>
            </tr>
            <tr>
                <th>
                    更多信息
                </th>
                <td colspan="3">
                    <a href="{{ host }}/module/monitorDetail/{{ monitor.id }}/" style="margin-right: 5px">报警详情</a>
                    <a href="{{ host }}/module/monitorStat/{{ monitor.id }}/" style="margin-right: 5px">历史指标</a>
                    <a href="{{ host }}/module/createMonitor/{{ monitor.id }}/" style="margin-right: 5px">查看配置</a>
                    {% if detector_task %}
                    <a href="{{ host }}/detector/detectTaskReport/{{ detector_task.id }}/" style="margin-right: 5px">自动定位</a>
                    {% endif %}
                    {% if data_flow %}
                    <a href="{{ host }}/dataflow/dataFlowView/{{ data_flow.id }}/">数据流</a>
                    {% endif %}
                </td>
            </tr>

        </table>
    </td></tr>
    <tr><td style="padding-top:20px;">
        <table align="center" border="1" cellpadding="0" cellspacing="0" width="600" style="border-collapse: collapse;">
            {% if custom_msg %}
            <tr>
                <th colspan="4">用户信息</th>
            </tr>
            <tr>
                <td colspan="4" style="word-break: break-all;">{{ custom_msg|safe }}</td>
            </tr>
            {% endif %}
            {% if error_condition_list %}
            <tr>
                <th colspan="4">异常指标</th>
            </tr>
            {% endif %}
            {% for condition in error_condition_list %}
            <tr>
                <th>报警指标</th>
                <td colspan="3">
                    <font color="red">
                    {{ condition.stat.desc }}
                    {% if "part" in condition.stat %} ({{ condition.stat.part }}){% endif %}
                    </font>
                </td>
            </tr>
            {% if "group" in condition.stat and condition.stat.group %}
            <tr>
                <th>{{ condition.stat.group_name }}</th>
                <td colspan="3">
                    <font color="red">
                    {{ condition.stat.group }}
                    </font>
                </td>
            </tr>
            {% endif %}
            <tr>
                <th>报警条件</th>
                <td>
                    <font color="red">
                    {{ condition.threshold_type }} {{ condition.threshold.operator }} {{ condition.threshold.value }}
                    </font>
                </td>
                <th>实际值</th>
                <td>
                    <font color="red">
                    {% if condition.value == None %}null{% else %}{{ condition.value }}{% endif %}
                    {{ condition.original }}
                    </font>
                </td>
            </tr>
            {% endfor %}
            {% if other_condition_list %}
            <tr>
                <th colspan="4">正常指标</th>
            </tr>
            {% endif %}
            {% for condition in other_condition_list %}
            <tr>
                <th>报警指标</th>
                <td colspan="3">
                    {{ condition.stat.desc }}
                    {% if "part" in condition.stat %} ({{ condition.stat.part }}){% endif %}
                </td>
            </tr>
            {% if "group" in condition.stat and condition.stat.group %}
            <tr>
                <th>{{ condition.stat.group_name }}</th>
                <td colspan="3">
                    {{ condition.stat.group }}
                </td>
            </tr>
            {% endif %}
            <tr>
                <th>报警条件</th>
                <td>
                    {{ condition.threshold_type }} {{ condition.threshold.operator }} {{ condition.threshold.value }}
                </td>
                <th>实际值</th>
                <td>
                    {% if condition.value == None %}null{% else %}{{ condition.value }}{% endif %}
                    {{ condition.original }}
                </td>
            </tr>
            {% endfor %}
        </table>
    </td></tr>
    </table>
</body>


        data_flow = self.data

        render_text = {
            "data_flow": data_flow,
            "custom_msg": self.monitor_task.error_msg,
            "host": settings.HOST,
            "monitor": self.monitor,
            "monitor_alert": self.monitor_alert,
            "error_condition_list": error_condition_list,
            "other_condition_list": other_condition_list,
            "check_time": self.alarm_log.check_time,
            "begin_time": self.begin_time,
            "level_name": self.level_name,
            "continue_time": continue_time,
            "detector_task": detector_task,
        }

        email_body = os.path.join(base_alarm.ALARM_PATH, "templates/module_monitor_email.html")
        t = Template(open(email_body).read())
        html = t.render(Context(render_text))
        return html

    def send_remind_email(self):

        """
        根据data的内容，发送邮件，并记录日志。

        :param data: 发送的订阅数据，格式如下：
        {
            "xulei12":      # 要发送的用户
                [
                    {
                        "indicator": "pv":   # 指标名
                        "this_value": 123,      # 今日/本周值
                        "last_value": 120,      # 昨日/上周值
                        "diff_rate": 0.02,      # 日/周环比
                        "week_diff_rate": 0.01, # 周同比
                        "week_avg": 122,        # 周均
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
        text = ''
        cc = 'kgdc-dev@baidu.com'
        for user in data:
            index_list = []
            for user_name, msg in user.items():
                email_addr = '%s@baidu.com' % user_name
                for index, changes in msg.items():
                    index_list.append(index)
                    for change, value in changes.items():
                        print change, value
                        # text = u'<html><h1>你好</h1></html>'
                index_str = ''
                for index_unicode in index_list:
                    index_str += index_unicode.encode('utf-8')
                title = u'订阅指标【%s】有更新' % index_str
                tools.send_email(email_addr, title.encode('utf-8'), text.encode('utf-8'), True)
        #print data

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
