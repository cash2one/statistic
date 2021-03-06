# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : spo_import.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import os
import time
import logging
# 第三方库

# 自有库
from lib import tools
from conf import conf
import source_config
import db


def convert_path(path, date):
    return path.replace("${DATE}", date)


def build_srcid_map(srcid_path, all_pv):
    srcid_query_map = {}
    srcid_pv_map = {}
    srcid_effect_map = {}
    with open(srcid_path) as fp:
        for line in fp:
            line = line.rstrip("\r\n").decode("utf-8")
            srcid, query, pv = line.split("\t")
            pv = float(pv)
            srcid_pv_map[srcid] = srcid_pv_map.get(srcid, 0) + pv
            if srcid not in srcid_query_map:
                srcid_query_map[srcid] = []
            srcid_query_map[srcid].append({"query":query, "pv":pv})
    if all_pv:
        srcid_effect_map = {srcid: float(pv) / all_pv for srcid, pv in srcid_pv_map.items()}
    # srcid_query_map只保留前10的query
    for query, query_pv in srcid_query_map.items():
        query_pv.sort(key=lambda x:x["pv"], reverse=True)
        if len(query_pv) > 10:
            query_pv = query_pv[:10]
        srcid_query_map[query] = query_pv
    return srcid_query_map, srcid_pv_map, srcid_effect_map
    

def import_data(date, side, path):
    """
    xulei12@baidu.com
    2016-07-04 大搜PV数据获取不到，改写逻辑
    :param date:
    :param side:
    :param path:
    :return:
    """
    pv_ftp = convert_path(source_config.SPO_SRC[side]["pv"], date)
    srcid_ftp = convert_path(source_config.SPO_SRC[side]["srcid"], date)
    pv_path = os.path.join(path, "pv.%s" % source_config.SIDE_NAME[side])
    srcid_path = os.path.join(path, "srcid.%s" % source_config.SIDE_NAME[side])
    pv_success = True
    try:
        # 获取大搜PV数据
        tools.wget(pv_ftp, pv_path)
    except:
        logging.info(u"下载PV数据失败！")
        pv_success = False
    try:
        tools.wget(srcid_ftp, srcid_path)
    except:
        logging.info(u"下载srcid数据失败！")
        return

    if pv_success:
        pv = open(pv_path).read().rstrip("\r\n")
        pv = float(pv)
    else:
        pv = None

    srcid_query_map, srcid_pv_map, srcid_effect_map = build_srcid_map(srcid_path, pv)

    stat_db = db.SaveDataBase(date, side)
    stat_db.clear_spo_srcid_stat()
    stat_db.save_spo_srcid_stat("srcid_pv", srcid_pv_map.items())
    logging.info("srcid_pv number:%s" % len(srcid_pv_map))
    if srcid_effect_map:
        stat_db.save_spo_srcid_stat("srcid_effect", srcid_effect_map.items())
        logging.info("srcid_effect number:%s" % len(srcid_effect_map))
    else:
        logging.info("there is no srcid_effect data")
    stat_db.clear_spo_query_stat()
    query_stat_list = []
    for srcid, query_pv_list in srcid_query_map.items():
        for query_pv in query_pv_list:
            query_stat_list.append([srcid, query_pv["query"], query_pv["pv"]])
    logging.info("query stat number:%s" % len(query_stat_list))
    stat_db.save_spo_query_stat(query_stat_list)
    stat_db.close()


def main(side, date):
    try:
        time.strptime(date, "%Y%m%d")
    except:
        logging.info(u"日期格式错误:%s" % date)
        logging.info(u"日期必须是%Y%m%d格式")
        return
    logging.info(u"开始导入%s端%s问答指标" % (side, date))
    path = os.path.join(conf.DATA_DIR, date)
    if not os.path.exists(path):
        os.mkdir(path)
    if side == "pc":
        try:
            import_data(date, source_config.PC_SIDE, path)
        except:
            logging.exception('')
            logging.info(u"pc端数据导入失败")
    elif side == "wise":
        try:
            import_data(date, source_config.WISE_SIDE, path)
        except:
            logging.exception('')
            logging.info(u"wise端数据导入失败")
    logging.info(u"问答指标导入结束")
