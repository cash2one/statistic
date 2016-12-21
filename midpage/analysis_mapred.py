#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：
本地执行。不是在hadoop集群上执行的代码。
以集群方式分析日志
File   : analysis_mapred.py.py

Authors: xulei12@baidu.com
Date   : 2016/12/19
Comment: 
"""
# 系统库
import os
import json
import logging
from multiprocessing import Process
# 第三方库

# 自有库
import analysis
from conf import conf
import midpagedb
import statist

LOG_DATAS = {
    "hanyu": "/app/ps/spider/wdmqa/kgdc/%s/hanyu/input"
    # "hanyu": "/app/ps/spider/wdmqa/kgdc/%s/hanyu/input"
}


def save_mapred_to_db(file_path):
    """
    将用户路径数据存入mongo数据库
    :param file_path:
    :return:
    """
    db = midpagedb.DateLogDb()
    logs = []
    path_nums = 0
    uid_nums = 0
    error_nums = 0
    cached_nums = 0
    with open(file_path) as fp:
        for line in fp:
            try:
                line = line.split("\t")
                line_log = json.loads(line[1])
                logs.append(line_log)
                if line[0] == "_uid":
                    uid_nums += 1
                else:
                    path_nums += 1
                cached_nums += 1
                if cached_nums > 500:
                    db.insert_log(logs)
                    logs = []
                    cached_nums = 0
            except Exception as e:
                error_nums += 1
                continue
        if cached_nums:
            db.insert_log(logs)
    file_name = os.path.split(file_path)[1]
    ret = {"file": file_name, "path": path_nums, "uid": uid_nums, "error": error_nums}
    print ret
    return ret


def run_mapred(source, in_path, date):
    """

    :param source:
    :param in_path:
    :param date:
    :return:
    """
    # /app/ps/spider/wdmqa/20161215/output
    out_path = "/app/ps/spider/wdmqa/kgdc/%s/%s/output" % (date, source)

    # 清理output目录
    cmd = conf.HADOOP_BIN + " fs -rmr %s" % out_path
    logging.info(cmd)
    os.system(cmd)

    # 执行hadoop任务
    current_dir = os.path.split(os.path.abspath(__file__))[0]
    cmd = "cd %s; sh ./run_job.sh %s %s %s %s " % (
        current_dir,
        conf.HADOOP_BIN,
        source,
        in_path,
        out_path)
    logging.info(cmd)
    os.system(cmd)
    # 获取hadoop运行结果
    root_path = os.path.join(conf.DATA_DIR, "midpage_hadoop")
    local_hadoop_path = os.path.join(root_path, date, source)
    if os.path.exists(local_hadoop_path):
        # 如果有文件先删除
        cmd = "rm -rf %s/*" % local_hadoop_path
        logging.info(cmd)
        os.system(cmd)
    else:
        os.makedirs(local_hadoop_path)

    # 下载文件
    cmd = conf.HADOOP_BIN + " fs -get %s %s" % (out_path, local_hadoop_path)
    logging.info(cmd)
    os.system(cmd)
    # 解析入库
    plist = []
    # hadoop输出目录带个output
    local_hadoop_path_output = os.path.join(local_hadoop_path, "output")
    for file_name in os.listdir(local_hadoop_path_output):
        file_path = os.path.join(local_hadoop_path_output, file_name)
        if os.path.isfile(file_path):
            p = Process(target=save_mapred_to_db, args=(file_path,))
            plist.append(p)
            p.start()

    for p in plist:
        p.join()


def main(date, sources=None):
    """

    :param date:
    :param sources:
    :return:
    """
    if sources:
        sources = sources.split(',')
    else:
        sources = LOG_DATAS.keys()
    # 设置mongo日期，在statist里面还会再设置一次
    midpagedb.DateLogDb.set_date(date)
    # 清空现有数据库
    analysis.clear_db(sources)
    # 清空统计用户路径数据
    if sources:
        user_path_sources = ["_user_path_" + source for source in sources]
        analysis.clear_db(user_path_sources)

    logging.info("开始解析日志....")
    # 跑hadoop任务，统计用户路径数据
    plist = []
    for source in sources:
        in_path = LOG_DATAS[source]
        if "%s" in in_path:
            in_path = in_path % date
        p = Process(target=run_mapred, args=(source, in_path, date))
        plist.append(p)
        p.start()

    for p in plist:
        p.join()
    # # 开启全量统计
    statist.main(date, "hadoop", sources)

