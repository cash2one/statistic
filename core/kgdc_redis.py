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


logger = logging.getLogger("sys")


class KgdcRedis(object):
    """
    负责与redis通信，对外提供业务级接口
    """
    def __init__(self, host=conf.REDIS_HOST,
                 port=conf.REDIS_PORT,
                 db=conf.REDIS_DB):
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
        # 客户端将结果放置的位置key
        self.key_setting = {
            "alarm": "alarm_queue",
            "timely": "timely_data"
        }

    def pop_item(self, key):
        """
        从队列中弹出一条数据
        :param key: 队列key
        :return:
        """
        unicode_value = self.rc.blpop(key, timeout=0)
        if unicode_value is not None:
            unicode_value = unicode_value[1]
            logger.info(unicode_value)
        return unicode_value

    def push_item(self, key, value):
        """
        往队列里写入一个数据
        :param key: 队列key
        :param value: 写入的值
        :return:
        """
        if not isinstance(value, unicode):
            logger.info("非unicode")
            return
        self.rc.rpush(key, value)
        logger.info(value)

    def pop_alarm(self):
        """
        获取监控项结果
        :return: 监控项结果数据，格式待定
        """
        return self.pop_item(self.key_setting["alarm"])

    def push_alarm(self, value):
        """
        提交监控项计算输出结构，内部控制防止重复提交机制，调用其他接口
        :param value: 监控结果，格式待定
        :return:
        """
        self.push_item(self.key_setting["alarm"], value)

    def pop_timely_data(self):
        """
        获取时效性数据
        :return: 监控项结果数据，格式待定
        """
        return self.pop_item(self.key_setting["timely"])

    def push_timely_data(self, value):
        """
        提交时效性入库数据
        :param value: 监控结果，格式待定
        :return:
        """
        self.push_item(self.key_setting["timely"], value)
