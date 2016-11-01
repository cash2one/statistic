#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : sender.py

Authors: xulei12@baidu.com
Date   : 2016/9/26
Comment:

Authors: xulei12@baidu.com
Date   : 2016/11/01
Comment: 增加通告平台的hi，sms报警。
"""
# 系统库
import json
import logging
# 第三方库
import jinja2

# 自有库
import lib.tools
import conf.conf as conf
import core.hermes


def send_remind_email(alarm_set):
    cc = 'kgdc-dev@baidu.com'
    env = jinja2.Environment(loader=jinja2.PackageLoader("alarm", 'templates'))

    if "receiver" in alarm_set["alert"]:
        user_list = alarm_set["alert"]["receiver"].split(";")
    else:
        user_list = []

    if "receiver_group" in alarm_set["alert"]:
        group_list = alarm_set["alert"]["receiver_group"].split(";")
    else:
        group_list = []
    user_list += group_list

    if not user_list:
        logging.warning("receiver and receive_group is empty")
        return
    email_list = []
    for item in user_list:
        if item:
            item += "@baidu.com"
        email_list.append(item)
    email_address = ";".join(email_list)

    title = u"【KGDC指标报警】【%s-%s】的指标“%s”超限"\
            % (alarm_set["project"]["project_name"],
               alarm_set["project"]["sub_project_name"],
               alarm_set["indicator"]["name"])
    if conf.DEVELOPING:
        title += u"【测试环境】"
    template = env.get_template('alarm_email.html')
    try:
        body = template.render(monitor=alarm_set["monitor"],
                               project=alarm_set["project"],
                               indicator=alarm_set["indicator"],
                               condition=alarm_set["alarm"]["condition"],
                               alarm=alarm_set["alarm"])
        lib.tools.send_email(email_address, title, body, True, cc=cc)
    except:
        logging.error(alarm_set)
        logging.exception("发送邮件错误")
    logging.info('send email to user %s :\n%s' % (email_address, body))


def send_by_hermes(alarm_set):
    """
    通告平台发送方式
    :param alarm_set:
    :return:
    """
    alert_list = []
    desc_list = []
    for item in alarm_set["alarm"]["condition"]:
        desc = calculate_desc_one(item)
        if desc:
            desc_list.append(desc)
    if alarm_set["alarm"]["logic"] == "and":
        all_desc = u"\n    ".join(desc_list)

    if "sms" in alarm_set["alert"]["channel"]:
        description = u"【KGDC】【%s-%s】【%s】%s" % (
            alarm_set["project"]["project_name"],
            alarm_set["project"]["sub_project_name"],
            alarm_set["indicator"]["name"],
            desc_list[0])
        if conf.DEVELOPING:
            description += u"【测试环境】"
        alert = {
            "channel": "sms",
            "description": description,
            "receiver": alarm_set["alert"]["receiver"]
        }
        alert_list.append(alert)
    if "hi" in alarm_set["alert"]["channel"]:
        description = u"【KGDC指标报警】\n"
        if conf.DEVELOPING:
            description += u"【测试环境】\n"
        description += u"【项目名】%s-%s\n" % (
            alarm_set["project"]["project_name"],
            alarm_set["project"]["sub_project_name"])
        description += u"【指标名】%s-%s\n" % (
            alarm_set["indicator"]["name"],
            alarm_set["indicator"]["id"])
        description += u"【指标维度】%s\n" % (
            json.dumps(alarm_set["indicator"]["dimension"], ensure_ascii=False))
        description += u"【报警详情】\n    "
        description += all_desc
        alert = {
            "channel": "hi",
            "description": description,
            "receiver": alarm_set["alert"]["receiver"]
        }
        alert_list.append(alert)
    core.hermes.send(alert_list)


def calculate_desc_one(condition):
    """
    描述语句生成
    :param condition:
    :return:
    """
    desc = u""
    name_map = {
        "minute": u"分钟",
        "hour": u"小时",
        "day": u"天",
        "week": u"星期",
    }
    if condition["alarm"]:
        if condition["type"] == "relative":
            if condition["time"]["unit"] in name_map:
                condition["time"]["unit"] = name_map[condition["time"]["unit"]]
            desc += u"相对值比%s%s前%s" % (
                condition["time"]["num"], condition["time"]["unit"], condition["operator"])
            if condition["percent"]:
                desc += str(condition["value"] * 100) + "%"
            else:
                desc += str(condition["value"])
        else:
            desc += u"绝对值%s%s" % (condition["operator"], condition["value"])
    return desc


def test_calculate_desc_one():
    """
    测试程序
    :return:
    """
    conditions = [{
        "type": "relative",
        "percent": False,
        "operator": ">",
        "value": 20,
        "time": {
            "unit": "小时",
            "num": 3,
        },
        "last_time": "2016-09-22 12:00",
        "last_value": 0.00000111122312308457,
        "diff_value": 0.000000123412345623112231,
        "alarm": True
    }, {
        "type": "relative",
        "percent": True,
        "operator": "<",
        "value": -0.5,
        "time": {
            "unit": "天",
            "num": 1,
        },
        "last_time": "2016-09-22 12:00",
        "last_value": 0.0982111122312308457,
        "diff_value": 0.00123412345623112231,
        "alarm": True
    }, {
        "type": "absolute",
        "operator": ">",
        "value": 100,
        "alarm": True
    }]
    for item in conditions:
        print calculate_desc_one(item)


def test():
    alert = {
        "alert": {
            "channel": ["sms", "hi", "email"],
            "receiver": "xulei12",
            "receiver_group": "",
        },
        "monitor": {

        },
        "indicator": {
            "name": "测试指标",
            "id": "pex",
            "value": 50,
            "create": "2016-09-26 14:23",
            "dimension": {

            }
        },
        "flag": True,
        "condition": {
            "type": "absolute",
            "percent": True,
            "operator": "<",
            "value": 40,
            "time": {
                "unit": "day",
                "num": 1
            }
        },
        "project": {
            "@subProject": 13,
            "project_name": "项目",
            "sub_project_name": "子项目",
            "page_id": 13
        },
        "alarm": {
            "logic": "and",
            "condition": [{
                "type": "relative",
                "percent": False,
                "operator": ">",
                "value": 20,
                "time": {
                    "unit": "小时",
                    "num": 3,
                },
                "last_time": "2016-09-22 12:00",
                "last_value": 0.00000111122312308457,
                "diff_value": 0.000000123412345623112231,
                "alarm": True
            }, {
                "type": "relative",
                "percent": True,
                "operator": "<",
                "value": -0.5,
                "time": {
                    "unit": "天",
                    "num": 1,
                },
                "last_time": "2016-09-22 12:00",
                "last_value": 0.0982111122312308457,
                "diff_value": 0.00123412345623112231,
                "alarm": True
            }, {
                "type": "absolute",
                "operator": ">",
                "value": 100,
                "alarm": True
            }]
        }
    }
    import json
    a = json.dumps(alert, ensure_ascii=False)
    alert = json.loads(a)
    send_remind_email(alert)
    send_by_hermes(alert)
