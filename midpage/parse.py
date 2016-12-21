#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : parse.py

Authors: xulei12@baidu.com
Date   : 2016/12/14
Comment: 
"""
# 系统库
import re
import json
import logging
import urlparse
# 第三方库

# 自有库


# 需要过滤掉静态文件信息的资源
NEED_FILTER = ["hanyu"]

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


class ParseLineError(Exception):
    """错误的行，会被略过
    """
    pass


def parse_query(query):
    """
    解析query
    :param query:
    :return:
    """
    query = query.encode('utf-8')
    query = urlparse.parse_qs(query)
    try:
        query = {k.decode('utf-8'): v[0].decode('utf-8') for k, v in query.items() if "." not in k}
    except:
        try:
            query = {k.decode('cp936'): v[0].decode('cp936') for k, v in query.items() if
                     "." not in k}
        except:
            logging.info("[ERROR QUERY]%s" % query)
            return {}
    return query


def parse_request(request, ret):
    """
    解析request
    :param request:
    :param ret:
    :return:
    """
    request = request.split()
    if len(request) == 3:
        request = request[1]
    else:
        raise ParseLineError('parse_request error')
    request = urlparse.urlparse(request)
    ret['url'] = request.path
    if len(ret['url']) > 1024:
        raise ParseLineError('url too long:%s' % ret['url'])
    ret['query'] = parse_query(request.query)

    for field in ret['query']:
        if field.endswith('_num'):
            try:
                ret['query'][field] = int(ret['query'][field])
            except:
                raise ParseLineError('[%s]query int error:%s' % (field, ret['query'][field]))
    if 'duration' in ret['query']:
        try:
            ret['query']['duration'] = float(ret['query']['duration'])
        except:
            raise ParseLineError('duration float error:%s' % ret['query']['duration'])
    if 'extend' in ret['query']:
        try:
            ret['query']['extend'] = json.loads(ret['query']['extend'])
        except:
            logging.exception('')
            raise ParseLineError('extend json.loads error:%s' % ret['query']['extend'])
        for key in ret['query']['extend']:
            if key.endswith('_num'):
                try:
                    ret['query']['extend'][key] = float(ret['query']['extend'][key])
                except:
                    raise ParseLineError('[%s]extend float error:%s' % (key, \
                                                                              ret['query'][
                                                                                  'extend'][key]))


def parse_user_agent(user_agent, ret):
    """
    判断用户设备
    :param user_agent:
    :param ret:
    :return:
    """
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
    """
    解析cookie
    :param cookie:
    :param ret:
    :return:
    """
    bdid = BAIDUID_REG.search(cookie)
    if bdid:
        ret["baiduid"] = bdid.group("id")
    else:
        ret["baiduid"] = ""


def analysis_qianxun(match, ret):
    """
    主要解析入口
    :param match:
    :param ret:
    :return:
    """
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
    """
    明星解析入口。暂时废弃不用
    :param match:
    :param ret:
    :return:
    """
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


def need_to_filter(line, source):
    """
    判断某一行解析后，是否需要被过滤掉不入库
    :param line:
    :param source:
    :return:
    """
    if source in NEED_FILTER:
        if int(line["status_code"]) > 299:
            return True
        # 无用url静态资源信息过滤
        url_suffixes = [".js", ".css", ".gif", ".png", ".jpg", ".jpeg", ".tiff", ".php"]
        for url_suffix in url_suffixes:
            if line["url"].endswith(url_suffix):
                return True
        # spider无用请求过滤
        spider_agent = "Baiduspider"
        if spider_agent in line["user_agent"]:
            return True
    return False


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
        if need_to_filter(ret, source):
            ret = None
    except ParseLineError as e:
        logging.info("[ParseLineError]%s" % e.message)
        # logging.info("[ERROR LOG][%s]%s" % (source, line))
        return
    return ret


def test():
    """
    测试代码
    :return:
    """
    line = '117.185.18.145 - - [13/Dec/2016:23:59:56 +0800] "GET /dumi/gaokao_search?province=' \
           '%E5%AE%89%E5%BE%BD&majorType=%E7%90%86%E7%A7%91&score=650&targetObject=major' \
           '&collegeName=%E5%8D%97%E4%BA%AC%E5%A4%A7%E5%AD%A6&from=ala-dumi-will HTTP/1.1"' \
           ' 200 4329 "http://duer.baidu.com/midpage/gaokao_search?targetObject=college&score=650' \
           '&province=%E5%AE%89%E5%BE%BD&majorType=%E7%90%86%E7%A7%91&from=ala-dumi-will&page=0' \
           '&anchor=%E4%BF%9D%E5%BA%95&json=0" "BIDUPSID=F073A1918C33C30327C2EB43E4A1A1BB;' \
           ' BAIDUID=F073A1918C33C30327C2EB43E4A1A1BB:FG=1; H_WISE_SIDS=107502_100616_100121_' \
           '109786_112044_100101_104381_103550_111979_110004_113483_110710_112200_112136_111286_' \
           '113932_113547_110011_113568_111549_113566_111325_112107_111927_112212_110654_107313_' \
           '111939_112134_110031_111216_110085" "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn;' \
           ' A31c Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/' \
           '37.0.0.0 MQQBrowser/7.1 Mobile Safari/537.36" 0.462 3596424245' \
           ' 117.185.18.145 10.143.242.16 unix:/home/work/odp/var/php-cgi.sock duer.baidu.com' \
           ' "183.162.34.118, 117.185.18.145" odp newapp 35964242450284331786121323 1481644796.885'
    print analysis_line(line, "qianxun")
