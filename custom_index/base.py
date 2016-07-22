# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : base.py

Authors: yangxiaotong@baidu.com
Date   : 2016/5/31
Comment: 
"""
# 标准库
import os
import time
import logging
import datetime
# 第三方库

# 自有库
from conf import conf
from lib import tools


def get_index(task_id, date, ftp):
    folder = os.path.join(conf.DATA_DIR, "custom_index/%s" % date)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, "%s.dat" % task_id)
    if "%s" in ftp:
        ftp = ftp % date
    tools.wget(ftp, path)
    return path


def check_line(json_line, must_keys):
    for key in must_keys:
        if key not in json_line:
            logging.error("[ERROR]miss key:%s=>%s" % (key, json_line))
            return False
    return True


def merge_date_time(str_date, str_time):
    """
    将2016-07-06与15:33合并成 2016-07-06 15:33
    xulei12@baidu.com
    2016.07.06
    :param str_date:
    :param str_time:
    :return:
    """
    return str_date + " " + str_time


def merge_date_time2ts(str_date, str_time):
    """
    将2016-07-18与15:33合并成ts  1468827180.0
    xulei12@baidu.com
    2016.07.18
    :param str_date: 2016-07-18
    :param str_time: 15:33
    :return:1468827180.0
    """
    time_str = str_date + " " + str_time
    time_obj = time.strptime(time_str, "%Y-%m-%d %H:%M")
    time_ts = time.mktime(time_obj)
    return time_ts


def date_time_str2utc(str_date, str_time):
    """
    将2016-07-06 05:00 这样的北京时间，转换为utc时间格式    2016-07-05 21:00
    xulei12@baidu.com
    2016.07.06
    :param str_date: 2016-07-06
    :param str_time: 05:00
    :return:返回值为一个datetime类，但是可以直接打印或使用，格式为  2016-07-05 21:00
    """
    list_date = str_date.split("-")
    year = int(list_date[0])
    month = int(list_date[1])
    day = int(list_date[2])
    str_time = str_time.split(":")
    hour = int(str_time[0])
    minute = int(str_time[1])
    ret = datetime.datetime(year, month, day, hour, minute)
    ret = ret - datetime.timedelta(hours=8)
    return ret


def utc_str2date_time(utc):
    """
    将一个utc时间的字符串，转换成北京时间的 datetime类
    xulei12@baidu.com
    2016.07.06
    :param utc: 字符串或者datetime，2016-07-06 03:41
    :return: 返回值为一个datetime类，但是可以直接打印或使用，格式为  2016-07-06 11:41
    """
    if isinstance(utc, type("")):
        utc = time.strptime(utc, "%Y-%m-%d %H:%M:%S")
        utc = datetime.datetime(utc.tm_year, utc.tm_mon, utc.tm_mday, utc.tm_hour, utc.tm_min)
    ret = utc + datetime.timedelta(hours=8)
    return ret


def get_diff_rate(new_data, old_data, ndigits=None):
    """
    计算差异百分比。统一成字符串输出格式
    :param new_data:
    :param old_data:
    :param ndigits:
    :return:
    """
    try:
        new_data = float(new_data)
        old_data = float(old_data)
    except Exception as e:
        return "-"
    if not new_data or not old_data:
        return "-"
    ret = (new_data - old_data)*100 / old_data
    if ndigits:
        ret = round(ret, ndigits)
    return str(ret) + "%"


def json_list_find(json_list, query, find_all=False):
    """
    类似前端的_.find接口，从json_list中找到符合条件query的项
    xulei12@baidu.com 2016-07-18
    :param json_list:  [{},{}]
    :param query: {}
    :param find_all: false-只查找一个，true-查找所有
    :return:
    """
    if not isinstance(json_list, list):
        return {}

    ret = []
    for item in json_list:
        if json_match(item, query):
            ret.append(item)
            if not find_all:
                return item
    return ret if find_all else {}


def json_match(data, query):
    """
    判断date是否符合query的要求
    xulei12@baidu.com 2016-07-18
    :param data:
    :param query:
    :return:
    """
    for (key, value) in query.items():
        if data[key] != value:
            return False
    return True


def json_list_sum_by(json_list, key):
    """
    类似前端接口 sumBy， 将key字段的所有值求和
    :param json_list:
    :param query:
    :return:
    """
    ret = 0
    for item in json_list:
        try:
            ret += float(item[key])
        except:
            continue
    return ret
