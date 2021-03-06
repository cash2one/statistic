# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_summary_data.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import re
import os
import logging
# 第三方库

# 自有库
import db
from conf import conf
from lib import tools


def index_parse(line, stat_map):
    split = line.split("\t")
    if len(split) != 2:
        return
    try:
        split[1] = float(split[1])
    except:
        logging.info("error index line:%s" % line)
        return
    stat_name = {
        "x": "total_click_num",
        "y": "total_show_num",
        "z": "distinct_page_num",
        "m": "avg_click_postion",
        "n": "avg_show_postion",
    }
    key = stat_name.get(split[0])
    if key is None:
        return
    stat_map[key] = split[1]


def position_parse(line, position_list):
    split = line.split("\t")
    try:
        split[0] = int(split[0])
        split[1] = int(split[1])
        split[2] = int(split[2])
        split[3] = float(split[3])
    except:
        logging.info("error position line:%s" % line)
        return
    position_list.append(split[:4])


def detail_parse(line, url_list):
    split = line.split("\t")
    try:
        split[1] = int(split[1])
        split[2] = int(split[2])
        split = split[:3]
        split.append(float(split[1]) / split[2])
    except:
        logging.info("error position line:%s" % line)
        return
    url_list.append(split)


def import_data(side, date, path, source, product_id):
    try:
        tools.wget(source, path)
    except:
        logging.info(u"下载失败！")
        return
    stat_map = {}
    position_list = []
    url_list = []

    reg = re.compile(r"^\[(\w+)\]$")
    this_mod = None
    with open(path) as fp:
        for line in fp:
            line = line.rstrip("\r\n").decode("utf-8")
            match = reg.match(line)
            if match:
                this_mod = match.group(1)
                if this_mod not in ("index", "position", "detail"):
                    logging.info("unkown mod: %s" % this_mod)
                continue
            if this_mod == "index":
                index_parse(line, stat_map)
            elif this_mod == "position":
                position_parse(line, position_list)
            elif this_mod == "detail":
                detail_parse(line, url_list)
    #position_list截取前50
    position_list.sort(key=lambda x:x[0])
    position_list = position_list[:min(50, len(position_list))]

    #url_list去重
    # url_map = {url[0]: url for url in url_list}
    # url_list = url_map.values()
    #导入数据库
    stat_db = db.SaveDataBase(date, side)
    logging.info("stat number:%s" % len(stat_map))
    stat_db.clear_midpage_stat(product_id)
    stat_db.save_midpage_stat(product_id, stat_map.items())
    logging.info("position number:%s" % len(position_list))
    stat_db.clear_midpage_position_stat(product_id)
    stat_db.save_midpage_position_stat(product_id, position_list)
    logging.info("url number:%s" % len(url_list))
    stat_db.clear_midpage_url_stat(product_id)
    stat_db.save_midpage_url_stat(product_id, url_list)
    stat_db.close()


def main(product_id, date):
    path = os.path.join(conf.DATA_DIR, date)
    if not os.path.exists(path):
        os.mkdir(path)

    path = os.path.join(path, "midpage.%s" % product_id)

    stat_db = db.DataBase()
    product = stat_db.get_midpage_product(product_id)
    if product is None:
        logging.info(u"产品id不存在：%s" % product_id)
        return
    name = product["name"]
    side = product["side"]
    source = product["source"]
    source = source.replace("${DATE}", date)
    logging.info(u"产品%s(%s)开始导入:%s" % (name, product_id, date))
    try:
        import_data(side, date, path, source, product_id)
    except:
        logging.exception('')
        logging.info(u"产品%s(%s)导入失败" % (name, product_id))
    logging.info(u"产品%s(%s)导入结束" % (name, product_id))
