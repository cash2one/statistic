#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : analysis.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-20
Comment:

从ftp地址获取线上nginx日志，解析日志，并存入mongo，
随后等待statist.py模块对解析后的日志做指标统计.
"""
# 标准库
import os
import json
import logging
from multiprocessing import Process, Pool
# 第三方库

# 自有库
import parse
from lib import tools
from conf import conf
import midpagedb
import statist

LOG_DATAS = {
    'qianxun': {
        'bjyz-dumi-midpage0.bjyz.baidu.com':
            'ftp://cq01-testing-ps7165.cq01.baidu.com'
            '/home/work/dumi_online_access_log/bjyz-dumi-midpage0.bjyz.baidu.com/access_%s.log',
        'bjyz-dumi-midpage1.bjyz.baidu.com':
            'ftp://cq01-testing-ps7165.cq01.baidu.com'
            '/home/work/dumi_online_access_log/bjyz-dumi-midpage1.bjyz.baidu.com/access_%s.log',
        'nj03-mco-wise272.nj03.baidu.com':
            'ftp://cq01-testing-ps7165.cq01.baidu.com'
            '/home/work/dumi_online_access_log/nj03-mco-wise272.nj03.baidu.com/access_%s.log',
        'nj03-mco-wise274.nj03.baidu.com':
            'ftp://cq01-testing-ps7165.cq01.baidu.com'
            '/home/work/dumi_online_access_log/nj03-mco-wise274.nj03.baidu.com/access_%s.log',
    },
    # 'hanyu': {
    #     'cq01-kg-search0.cq01':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/cq01-kg-search0.cq01',
    #     'cq01-kg-search1.cq01':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/cq01-kg-search1.cq01',
    #     'bjyz-kg-web0.bjyz':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web0.bjyz',
    #     'bjyz-kg-web1.bjyz':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web1.bjyz',
    #     'bjyz-kg-web2.bjyz':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web2.bjyz',
    #     'bjyz-kg-web3.bjyz':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web3.bjyz',
    #     'nj02-kg-web0.nj02':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web0.nj02',
    #     'nj02-kg-web1.nj02':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web1.nj02',
    #     'nj02-kg-web2.nj02':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web2.nj02',
    #     'nj02-kg-web3.nj02':
    #         'ftp://yq01-kg-diaoyan13.yq01.baidu.com'
    #         '/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web3.nj02'
    # }
}


def clear_db(sources):
    db = midpagedb.DateLogDb()
    db.clear(sources)


def get_data(date, sources=None):
    """
    wget数据到本地
    :param date:
    :param sources:
    :return:
    """
    files = []
    # 清理过期超过7天的文件
    root_path = os.path.join(conf.DATA_DIR, "midpage")
    tools.clear_files(root_path, 7)
    # 获取数据
    midpage_dir = os.path.join(root_path, date)
    for source, log_dict in LOG_DATAS.items():
        if sources and source not in sources:
            continue
        log_dir = os.path.join(midpage_dir, source)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        for file_name, log_ftp in log_dict.items():
            try:
                file_name = os.path.join(log_dir, file_name)
                if '%s' in log_ftp:
                    log_ftp = log_ftp % date
                tools.wget(log_ftp, file_name, False)
                files.append({
                    "source": source,
                    "file_name": file_name,
                })
            except Exception as e:
                continue
    return files


def spilt_files(files):
    """
    对数据文件分割  大小1000000行 大约1G
    :param files:
    :return:
    """
    spilt_file = []
    for obj in files:
        source = obj["source"]
        file_name = obj["file_name"]
        index = 0
        line_count = 0
        path_file = os.path.split(file_name)
        print path_file[0], path_file[1]
        path = os.path.join(path_file[0], "%s_part%02d" % (path_file[1], index))
        fw = open(path, 'w')
        spilt_file.append({
            "source": source,
            "file_name": path
        })
        for line in open(file_name, 'r'):
            fw.write(line)
            line_count += 1
            # 假设每10000行写一个文件
            if line_count > 1000000:
                fw.close()
                index += 1
                line_count = 0
                path = os.path.join(path_file[0], "%s_part%02d" % (path_file[1], index))
                fw = open(path, 'w')
                spilt_file.append({
                    "source": source,
                    "file_name": path
                })
        fw.close()
    return spilt_file


def del_spilt_files(files):
    """
    删掉分割的文件
    :param files:
    :return:
    """
    for obj in files:
        file_name = obj["file_name"]
        os.remove(file_name)


def iter_file(files):
    """
    根据files里的配置，挨个读取文件，并按照行返回。
    :param files:
    :return:
    """
    for line in open(files):
        line = line.rstrip("\r\n").decode("utf-8")
        yield line


def process_file(source, file_name):
    """
    解析完的数据保存至mongodb
    :param source:
    :param file_name:
    :return:
    """
    db = midpagedb.DateLogDb()
    error_num = 0
    log_num = 0
    logs = []
    for line in iter_file(file_name):
        # 分析一行
        log_line = parse.analysis_line(line, source)
        if log_line:
            logs.append(log_line)
            log_num += 1
        else:
            error_num += 1
        # 500行写一次mongo
        if len(logs) > 500:
            db.insert_log(logs)
            logs = []
    if logs:
        db.insert_log(logs)
    logging.info("log num:%s" % log_num)
    logging.info("error log num:%s" % error_num)


def save_log(files):
    """
    解析完的数据保存至mongodb
    :param files:
    :return:
    """
    plist = []
    # pool = Pool(processes=10)
    for obj in files:
        source = obj["source"]
        file_name = obj["file_name"]
        # pool.apply_async(process_file, (source, file_name))     # 维持执行的进程总数为10
        p = Process(target=process_file, args=(source, file_name))
        plist.append(p)
        p.start()

    for p in plist:
        p.join()

        # pool.close()
        # pool.join()


def main(date, sources=None):
    if sources:
        sources = sources.split(',')
    else:
        sources = LOG_DATAS.keys()
    # 设置mongo日期，在statist里面还会再设置一次
    midpagedb.DateLogDb.set_date(date)
    # 清空现有数据库
    clear_db(sources)
    # 根据LOG_DATAS的配置，wget下数据，返回格式 [{"source": xxx, "file_name": xxx},{……}]
    files = get_data(date, sources)
    # 对数据文件分割  大小1000000行 大约1G
    spilt_file = spilt_files(files)
    # files = ['/home/work/kgdc-statist/kgdc-statist/data/20160111/midpage/nj02-kgb-haiou1.nj02']
    logging.info("开始解析日志....")
    # 进程池  10个进程 分析分割文件
    save_log(spilt_file)
    # 开启全量统计
    statist.main(date, sources=sources)
    # 删除分割的文件
    del_spilt_files(spilt_file)
