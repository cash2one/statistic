#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

Authors: xulei12@baidu.com
Data   : 2016/4/19

Data   : 2016/9/23
Info   : 从kgqc迁入，修改
"""
# 系统库
import json
import logging
# 第三方库
import redis
import conf.conf as conf


# 客户端将结果放置的位置key
RESULT_KEY = "alarm_queue"
logger = logging.getLogger("sys")


class KgdcRedis(object):
    """
    负责与redis通信，对外提供业务级接口
    """
    def __init__(self, host=conf.REDIS_HOST,
                 port=conf.REDIS_PORT,
                 db=conf.REDIS_DB,
                 key_setting=RESULT_KEY):
        """
        初始化, 写入以unicode编码。 可以传入自定义参数。不传则使用settings中的配置
        :param host: 服务器地址
        :param port: 端口
        :param db: 数据库编号
        :param key_setting: 队列key
        :return:
        """
        try:
            self.rc = redis.StrictRedis(host=host, port=port, db=db)
        except:
            logger.exception("failed to connect Redis")
        self.key_setting = key_setting

    def pop_alarm(self):
        """
        获取监控项结果
        :return: 监控项结果数据，格式待定
        """
        unicode_value = self.rc.blpop(self.key_setting, timeout=60)
        if unicode_value is not None:
            unicode_value = unicode_value[1]
            logger.info(unicode_value)
        return unicode_value

    def push_alarm(self, value):
        """
        提交监控项计算输出结构，内部控制防止重复提交机制，调用其他接口
        :param value: 监控结果，格式待定
        :return:
        """
        if not isinstance(value, unicode):
            logger.info("非unicode")
            return
        self.rc.rpush(self.key_setting, value)
        logger.info(value)
