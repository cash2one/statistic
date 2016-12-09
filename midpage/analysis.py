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
import re
import json
import time
import shutil
import logging
import urlparse
from multiprocessing import Process, Pool
# 第三方库

# 自有库
import error
from lib import tools
from conf import conf
import midpagedb
import statist



LOG_DATAS = {
    'qianxun': {
        # 'st01-kgb-haiou1.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/access_%s.log',
        # 'st01-kgb-haiou2.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/access_%s.log',
        # 'nj02-kgb-haiou1.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/access_%s.log',
        # 'nj02-kgb-haiou2.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/access_%s.log',
        'bjyz-dumi-midpage0.bjyz.baidu.com': 'ftp://cq01-testing-ps7165.cq01.baidu.com/home/work/dumi_online_access_log/bjyz-dumi-midpage0.bjyz.baidu.com/access_%s.log',
        'bjyz-dumi-midpage1.bjyz.baidu.com': 'ftp://cq01-testing-ps7165.cq01.baidu.com/home/work/dumi_online_access_log/bjyz-dumi-midpage1.bjyz.baidu.com/access_%s.log',
        'nj03-mco-wise272.nj03.baidu.com': 'ftp://cq01-testing-ps7165.cq01.baidu.com/home/work/dumi_online_access_log/nj03-mco-wise272.nj03.baidu.com/access_%s.log',
        'nj03-mco-wise274.nj03.baidu.com': 'ftp://cq01-testing-ps7165.cq01.baidu.com/home/work/dumi_online_access_log/nj03-mco-wise274.nj03.baidu.com/access_%s.log',
    },
    #'mingxing': {
    #    'nj02-kgb-haiou1.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/star_tongji_%s.log',
    #    'nj02-kgb-haiou2.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/star_tongji_%s.log',
    #    'st01-kgb-haiou1.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com:/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/star_tongji_%s.log',
    #    'st01-kgb-haiou2.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com:/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/star_tongji_%s.log',
    #},
    'qianxun_test': {
        #'access_log': 'ftp://cp01-rdqa04-dev111.cp01.baidu.com/home/users/wangyuntian/work/dumi-data/temp.log.%s',
    },
    # 'baidu_dictionary': {
    #     'baidu_dictionary.filename': 'ftp://cp01-xiongyue.epc.baidu.com/home/work/data/xiongyue'
    # },
    'baidu_hanyu': {
        'cq01-kg-search0.cq01': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/cq01-kg-search0.cq01',
        'bjyz-kg-web0.bjyz': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web0.bjyz',
        'bjyz-kg-web1.bjyz': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web1.bjyz',
        'bjyz-kg-web2.bjyz': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web2.bjyz',
        'bjyz-kg-web3.bjyz': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/bjyz-kg-web3.bjyz',
        'nj02-kg-web0.nj02': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web0.nj02',
        'nj02-kg-web1.nj02': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web1.nj02',
        'nj02-kg-web2.nj02': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web2.nj02',
        'nj02-kg-web3.nj02': 'ftp://yq01-kg-diaoyan13.yq01.baidu.com/home/disk0/kgdc-log-transfer/data/%s/hanyu/nj02-kg-web3.nj02'
    }
}


BASE_REG = re.compile(r"^([0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                      r"\"(?P<request>.*)\" (?P<status_code>[0-9]{3}) (\d+) "
                      r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" \"(?P<user_agent>.*)\" "
                      r"(?P<cost_time>[0-9\.]+) ([0-9]+) ([0-9\.]+) ([0-9\.]+) (.+) (.*) "
                      r"\"(.*)\" (\w*) (\w*) (\d+) (?P<timestamp>[0-9\.]+)$")

MINGXING_REG = re.compile(r"^(?P<ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                          r"\"(?P<request>.*)\" (?P<status_code>[0-9]{3}) (\d+) "
                          r"\"(?P<referr>.*)\" \"(?P<user_agent>.*)\"$")

REG_MAP = {
    'qianxun': BASE_REG,
    'mingxing': BASE_REG,

    # 'mingxing': MINGXING_REG,
}

BAIDUID_REG = re.compile(r"BAIDUID=(?P<id>.+?):(.*=\d*)(;|$)")
IOS_REG = re.compile(r"(?i)Mac OS X")
ANDROID_REG = re.compile(r"(?i)android")
NA_REG = re.compile(r"(xiaodurobot|dueriosapp|duerandroidapp)")
MB_REG = re.compile(r"baiduboxapp")


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
    # 清理过期超过30天的文件
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
        fw = open(path_file[0]+'/part'+str(index)+'-'+path_file[1], 'w')
        spilt_file.append({
                "source": source,
                "file_name": path_file[0]+'/part'+str(index)+'-'+path_file[1],
            })
        for line in open(file_name, 'r'):
            fw.write(line)
            line_count += 1
            # 假设每10000行写一个文件
            if line_count > 1000000:
                fw.close()
                index += 1
                line_count = 0
                fw = open(path_file[0]+'/part'+str(index)+'-'+path_file[1], 'w')
                spilt_file.append({
                    "source": source,
                    "file_name": path_file[0]+'/part'+str(index)+'-'+path_file[1],
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


def parse_query(query):
    query = query.encode('utf-8')
    query = urlparse.parse_qs(query)
    try:
        query = {k.decode('utf-8'):v[0].decode('utf-8') for k, v in query.items() if "." not in k}
    except:
        try:
            query = {k.decode('cp936'):v[0].decode('cp936') for k, v in query.items() if "." not in k}
        except:
            logging.info("[ERROR QUERY]%s" % query)
            return {}
    return query


def parse_request(request, ret):
    request = request.split()
    if len(request) == 3:
        request = request[1]
    else:
        raise error.ParseLineError('parse_request error')
    request = urlparse.urlparse(request)
    ret['url'] = request.path
    if len(ret['url']) > 1024:
        raise error.ParseLineError('url too long:%s' % ret['url'])
    ret['query'] = parse_query(request.query)

    for field in ret['query']:
        if field.endswith('_num'):
            try:
                ret['query'][field] = int(ret['query'][field])
            except:
                raise error.ParseLineError('[%s]query int error:%s' % (field, ret['query'][field]))
    if 'duration' in ret['query']:
        try:
            ret['query']['duration'] = float(ret['query']['duration'])
        except:
            raise error.ParseLineError('duration float error:%s' % ret['query']['duration'])
    if 'extend' in ret['query']:
        try:
            ret['query']['extend'] = json.loads(ret['query']['extend'])
        except:
            logging.exception('')
            raise error.ParseLineError('extend json.loads error:%s' % ret['query']['extend'])
        for key in ret['query']['extend']:
            if key.endswith('_num'):
                try:
                    ret['query']['extend'][key] = float(ret['query']['extend'][key])
                except:
                    raise error.ParseLineError('[%s]extend float error:%s' % (key,\
                        ret['query']['extend'][key]))


def parse_user_agent(user_agent, ret):
    if IOS_REG.search(user_agent):
        ret["os"] = "ios"
    elif ANDROID_REG.search(user_agent):
        ret["os"] = "android"
    else:
        ret["os"] = "other"
    if NA_REG.search(user_agent):
        ret["client"] = "NA"
    elif MB_REG.search(user_agent):
        ret["client"] = "MB"
    else:
        ret["client"] = "other"


def parse_cookie(cookie, ret):
    bdid = BAIDUID_REG.search(cookie)
    if bdid:
        ret["baiduid"] = bdid.group("id")
    else:
        ret["baiduid"] = ""


def analysis_qianxun(match, ret):
    request = match.group("request")
    parse_request(request, ret)

    user_agent = match.group("user_agent")
    ret["user_agent"] = user_agent
    parse_user_agent(user_agent, ret)
    ret["status_code"] = match.group("status_code")
    ret["cost_time"] = match.group("cost_time")
    cookie = match.group("cookie")
    ret["cookie"] = cookie
    parse_cookie(cookie, ret)

    referr = match.group("referr")
    referr = urlparse.urlparse(referr)
    ret["referr"] = referr.path
    ret["referr_query"] = parse_query(referr.query)
    timestamp = match.group("timestamp")
    ret["timestamp"] = float(timestamp)
    

def analysis_mingxing(match, ret):
    request = match.group("request")
    parse_request(request, ret)
    user_agent = match.group("user_agent")
    ret["user_agent"] = user_agent
    parse_user_agent(user_agent, ret)
    ret["status_code"] = match.group("status_code")
    ip = match.group("ip")
    ret["baiduid"] = ip
    
    referr = match.group("referr")
    referr = urlparse.urlparse(referr)
    ret["referr"] = referr.path
    ret["referr_query"] = parse_query(referr.query)
    timestamp = match.group("time")
    timestamp = timestamp[1:-1]
    timestamp = timestamp.split()[0]
    ret["timestamp"] = float(time.mktime(time.strptime(timestamp, "%d/%b/%Y:%H:%M:%S")))


def analysis_line(line, source):
    """
    解析一行数据
    :param line:
    :param source:
    :return:
    """
    reg = REG_MAP.get(source)
    if reg is None:
        reg = BASE_REG
    match = reg.match(line)
    if match is None:
        logging.info("[NOT MATCH LOG]%s" % line)
        return
    ret = {'source': source}
    try:
        analysis_qianxun(match, ret)
        # if source == 'qianxun':
        #     analysis_qianxun(match, ret)
        # elif source == 'mingxing':
        #     analysis_mingxing(match, ret)
    except error.ParseLineError as e:
        logging.info("[ParseLineError]%s" % e.message)
        # logging.info("[ERROR LOG][%s]%s" % (source, line))
        return
    return ret


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
        log_line = analysis_line(line, source)
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
        sources = None
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


def test():
    """
    测试
    :return:
    """
    line = '183.49.122.252 - - [06/Dec/2016:20:01:54 +0800] "GET /static/asset" 400' \
           ' 0 "-" "-" "-" 0.034 0114' \
           '459466 183.49.122.252 10.205.56.23 - hanyu.baidu.com "-"   0114459466038959847' \
           '4120620 1481025714.459'
    print analysis_line(line, "baidu_hanyu")
