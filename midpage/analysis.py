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
"""
# 标准库
import os
import re
import json
import time
import urlparse
# 第三方库

# 自有库
import error
from lib import tools
from conf import conf
import midpagedb
import statist


LOG_DATAS = {
    'qianxun': {
        'st01-kgb-haiou1.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/access_%s.log',
        'st01-kgb-haiou2.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/access_%s.log',
        'nj02-kgb-haiou1.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/access_%s.log',
        'nj02-kgb-haiou2.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/access_%s.log',
    },
    'mingxing': {
        'nj02-kgb-haiou1.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/star_tongji_%s.log',
        'nj02-kgb-haiou2.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/star_tongji_%s.log',
        'st01-kgb-haiou1.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com:/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/star_tongji_%s.log',
        'st01-kgb-haiou2.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com:/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/star_tongji_%s.log',
    },
    'qianxun_test': {
        #'access_log': 'ftp://cp01-rdqa04-dev111.cp01.baidu.com/home/users/wangyuntian/work/dumi-data/temp.log.%s',
    },
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

BAIDUID_REG = re.compile(r"BAIDUID=(?P<id>.+?);")
IOS_REG = re.compile(r"(?i)Mac OS X")
ANDROID_REG = re.compile(r"(?i)android")
NA_REG = re.compile(r"(xiaodurobot|dueriosapp|duerandroidapp)")
MB_REG = re.compile(r"baiduboxapp")


def clear_db(sources):
    db = midpagedb.DateLogDb()
    db.clear(sources)


def get_data(date, sources=None):
    files = []
    midpage_dir = os.path.join(conf.DATA_DIR, "midpage/%s" % date)
    for source, log_dict in LOG_DATAS.items():
        if sources and source not in sources:
            continue
        log_dir = os.path.join(midpage_dir, source)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        for file_name, log_ftp in log_dict.items():
            file_name = os.path.join(log_dir, file_name)
            if '%s' in log_ftp:
                log_ftp = log_ftp % date
            tools.wget(log_ftp, file_name, False)
            files.append({
                "source": source,
                "file_name": file_name,
            })
    return files


def iter_file(files):
    for obj in files:
        source = obj["source"]
        file_name = obj["file_name"]
        for line in open(file_name):
            line = line.rstrip("\r\n").decode("utf-8")
            yield source, line


def parse_query(query):
    query = query.encode('utf-8')
    query = urlparse.parse_qs(query)
    try:
        query = {k.decode('utf-8'):v[0].decode('utf-8') for k, v in query.items() if "." not in k}
    except:
        try:
            query = {k.decode('cp936'):v[0].decode('cp936') for k, v in query.items() if "." not in k}
        except:
            tools.log("[ERROR QUERY]%s" % query)
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
    int_fields = ['image_num', 'review_num', 'tuangou_num']
    for field in int_fields:
        if field in ret['query']:
            try:
                ret['query'][field] = int(ret['query'][field])
            except:
                pass
    if 'duration' in ret['query']:
        try:
            ret['query']['duration'] = float(ret['query']['duration'])
        except:
            raise error.ParseLineError('duration float error:%s' % ret['query']['duration'])
    if 'extend' in ret['query']:
        try:
            ret['query']['extend'] = json.loads(ret['query']['extend'])
        except:
            tools.ex()
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
    number_keys = ["review_num", "image_num", "tuangou_num"]
    for number_key in number_keys:
        if number_key in ret["query"]:
            try:
                ret["query"][number_key] = int(ret["query"][number_key])
            except:
                pass
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
    reg = REG_MAP.get(source)
    if reg is None:
        reg = BASE_REG
    match = reg.match(line)
    if match is None:
        tools.log("[NOT MATCH LOG]%s" % line)
        return
    ret = {'source': source}
    try:
        analysis_qianxun(match, ret)
        # if source == 'qianxun':
        #     analysis_qianxun(match, ret)
        # elif source == 'mingxing':
        #     analysis_mingxing(match, ret)
    except error.ParseLineError as e:
        tools.log("[ParseLineError]%s" % e.message)
        tools.log("[ERROR LOG][%s]%s" % (source, line))
        return
    return ret


def save_log(files):
    logs = []
    db = midpagedb.DateLogDb()
    error_num = 0
    log_num = 0
    for source, line in iter_file(files):
        log_line = analysis_line(line, source)
        if log_line:
            logs.append(log_line)
            log_num += 1
        else:
            error_num += 1
        if len(logs) > 0:
            db.insert_log(logs)
            logs = []
    if logs:
        db.insert_log(logs)
    tools.log("log num:%s" % log_num)
    tools.log("error log num:%s" % error_num)


def main(date, sources=None):
    if sources:
        sources = sources.split(',')
    else:
        sources = None
    midpagedb.DateLogDb.set_date(date)
    clear_db(sources)
    files = get_data(date, sources)
    # files = ['/home/work/kgdc-statist/kgdc-statist/data/20160111/midpage/nj02-kgb-haiou1.nj02']
    tools.log("开始解析日志....")
    save_log(files)
    # 开启全量统计
    statist.main(date, sources=sources)
