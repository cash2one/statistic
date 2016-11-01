#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : hermes.py

Authors: xulei12@baidu.com
Date   : 2016/11/1
Comment: 
"""
# 系统库
import json
import urllib2
import logging
# 第三方库

# 自有库


def send(alert_list):
    """
    入口全部使用unicode

    {
        "appId": "12306",
        "token": "0bf8ca467a8bb395a1296df1fff55dd2",
        "alertList": [{
            "channel": "hi",
            "description": "hermes报警",
            "receiver": "liurongcheng;dingfei;sunyuelong;z_6031"
        }, {
            "channel": "sms",
            "description": "hermes报警",
            "level": "major",
            "receiver": "18686514206;liurongcheng;g_psop_szop-rd_rota;z_6031;
        }, {
            "channel": "phone",
            "description": "hermes报警",
            "receiver": "liurongcheng;dingfei;18686514206"
        }, {
            "channel": "email",
            "description": "hermes报警",
            "title": "hermes报警",
            "receiver": "liurongcheng;dingfei;liurongcheng@baidu.com"
        }]
    }
    :param alert_list:
    :return:
    """

    if type(alert_list) != list:
        logging.warning("alert_list is not a list")
        return False
    check_keys = ["channel", "description", "receiver"]

    for item in alert_list:
        for key in check_keys:
            if key not in item:
                logging.warning("%s not in one item of alert_list: %s"
                                % (key, json.dumps(item, ensure_ascii=False)))
                return False
    base_url = "http://jingle.baidu.com/alert/push"
    token = "572c7993833bd87a1c3c9869"
    app_id = "662"
    post_data = {
        "appId": app_id,
        "token": token,
        "alertList": alert_list
    }
    try:
        logging.info(json.dumps(post_data, ensure_ascii=False))
        r = urllib2.urlopen(url=base_url,
                            data=json.dumps(post_data, ensure_ascii=False).encode("utf-8"))
        ret = json.loads(r.read())
        logging.info(ret)
        if ret["code"] == '1000':
            return True
        else:
            return False
    except Exception as e:
        logging.exception(e)
        return False
