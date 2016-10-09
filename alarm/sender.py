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
"""
# 系统库
import logging
# 第三方库
import jinja2

# 自有库
import lib.tools
import conf.conf as conf


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
        title += "【测试环境】"
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


def test():
    alert = {
        "channel": ["sms", "hi", "email"],
        "receiver": "xulei12",
        "receiver_group": "",
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
                "last_value": 80,
                "diff_value": 0.323,
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
                "last_value": 80,
                "diff_value": 0.323,
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
