#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : base_mapred.py
mapred指标计算任务基类


Authors: xulei12@baidu.com
Date   : 2017/1/3
Comment: 
"""
# 系统库
import re
import os
import sys
import json
import urlparse
import hashlib
import logging
from importlib import import_module
# 第三方库

# 自有库
import conf.conf as conf


class BaseMapred(object):
    """
    执行hadoop任务的基类
    类里所有接口都是用于集群上执行。
    同时有构造测试流程
    """
    # 本地测试开关， 测试方法，在基类上继承一个新类，定义Test为True即可
    TEST = False
    # 指定日志来源。可以nanlin本地集群，可以为其他集群。其他集群会先执行distcp
    SOURCE = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310" \
             "/app/dt/minos/3/textlog/www/wise_tiyu_access/70025011/%s/1200/"
    # 本地测试时，存储结果的位置
    OUTPUT_FILE = ""
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^(?P<client_ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status>[0-9]+) ([0-9]+) "
                        r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" "
                        r"\"(?P<user_agent>.*)\" rt=(?P<cost_time>[0-9\.]+) [0-9]* "
                        r"([0-9\.]+) ([0-9\.]+) (.*) "
                        r"\".*\" ps appEngine - (?P<msec>[0-9\.]+)$")
    # cookie中搜索百度uid匹配规则
    BAIDUID_REG = re.compile(r"BAIDUID=(?P<id>.+?):(.*=\d*)(;|$)")
    # 设备匹配规则
    IOS_REG = re.compile(r"(?i)Mac OS X")
    ANDROID_REG = re.compile(r"(?i)android")
    NA_REG = re.compile(r"(xiaodurobot|dueriosapp|duerandroidapp)")
    MB_REG = re.compile(r"baiduboxapp")
    # 指标配置
    # 维度配置
    # 结果处理配置

    def __init__(self):
        """
        初始化
        :param mapred:
        :param source:
        :return:
        """
        self.valid_lines = 0
        self.not_match_lines = 0
        self.error_lines = 0

        if self.TEST:
            self.collection = []
            self.mapper_out = None
            self._mapper_in = self._mapper_in_test
            self._reducer_in = self._reduce_in_test
            self._emit = self._emit_test

    def _mapper_in(self):
        """

        :return:
        """
        return sys.stdin

    def _reducer_in(self):
        """

        :return:
        """
        return sys.stdin

    def _emit(self, key, value=None):
        """

        :param key:
        :param value:
        :return:
        """
        if value:
            print "%s\t%s" % (key, value)
        else:
            print key

    def _mapper_in_test(self):
        """
        本地测试用
        :return:
        """
        with open(self.SOURCE) as fp:
            for line in fp:
                line = line.rstrip()
                yield line

    def _reduce_in_test(self):
        """
        本地测试用
        :return:
        """
        return self.mapper_out

    def _emit_test(self, key, value=None):
        """
        本地测试用
        :return:
        """
        if value:
            self.collection.append("%s\t%s" % (key, value))
        else:
            m2 = hashlib.md5()
            m2.update(key)
            self.collection.append("%s\t%s" % (m2.hexdigest(), key))

    def parse_request(self, line):
        """
        解析request
        :param line:
        :return:
        """
        request = line["request"]
        request = request.split()
        if len(request) == 3:
            request = request[1]
        else:
            raise
        request = urlparse.urlparse(request)
        line['url'] = request.path
        if len(line['url']) > 1024:
            raise
        line['query'] = self.parse_query(request.query)

        for field in line['query']:
            if field.endswith('_num'):
                try:
                    line['query'][field] = int(line['query'][field])
                except:
                    raise
        if 'duration' in line['query']:
            try:
                line['query']['duration'] = float(line['query']['duration'])
            except:
                raise
        if 'extend' in line['query']:
            try:
                line['query']['extend'] = json.loads(line['query']['extend'])
            except:
                logging.exception('')
                raise
            for key in line['query']['extend']:
                if key.endswith('_num'):
                    try:
                        line['query']['extend'][key] = float(line['query']['extend'][key])
                    except:
                        raise

    def parse_query(self, query):
        """
        解析query
        :param query:
        :return:
        """
        query = query.encode('utf-8')
        query = urlparse.parse_qs(query)
        try:
            query = {k.decode('utf-8'): v[0].decode('utf-8')
                     for k, v in query.items() if "." not in k}
        except:
            try:
                query = {k.decode('cp936'): v[0].decode('cp936')
                         for k, v in query.items() if "." not in k}
            except:
                raise
        return query

    def parse_user_agent(self, line):
        """
        判断用户设备
        :param line:
        :return:
        """
        user_agent = line["user_agent"]
        if self.IOS_REG.search(user_agent):
            line["os"] = "ios"
        elif self.ANDROID_REG.search(user_agent):
            line["os"] = "android"
        else:
            line["os"] = "other"
        if self.NA_REG.search(user_agent):
            line["client"] = "NA"
        elif self.MB_REG.search(user_agent):
            line["client"] = "MB"
        else:
            line["client"] = "other"

    def parse_cookie(self, line):
        """
        解析cookie
        :param line:
        :param ret:
        :return:
        """
        cookie = line["cookie"]
        bdid = self.BAIDUID_REG.search(cookie)
        if bdid:
            line["baiduid"] = bdid.group("id")
        else:
            line["baiduid"] = ""

    def pars_referr(self, line):
        """

        :param line:
        :return:
        """
        referr = line["referr"]
        referr = urlparse.urlparse(referr)
        if "=" in referr.path:
            path = referr.path.split("/")
            line["referr"] = "/" + path[-1]
        else:
            line["referr"] = referr.path
        line["referr_query"] = self.parse_query(referr.query)

    def analysis_json(self, line):
        """
        对解析成json的字段进行进一步分析
        :param line:
        :return:
        """
        self.parse_request(line)
        self.parse_user_agent(line)
        self.parse_cookie(line)
        self.pars_referr(line)

    def analysis_line(self, line):
        """
        基类继承可能需要改写该方法
        :param line:
        :return:
        """
        reg = self.FILTER
        ret = None
        if reg:
            try:
                match = reg.match(line)
                if match:
                    self.valid_lines += 1
                    ret = match.groupdict()
                    self.analysis_json(ret)
                else:
                    self.not_match_lines += 1
            except Exception as e:
                self.error_lines += 1
        return ret

    def mapper(self):
        """
        方法需要在继承类上重写
        :return:
        """
        for line in self._mapper_in():
            try:
                line = line.strip()
                line = self.analysis_line(line)
                if line:
                    line_out = json.dumps(line, ensure_ascii=False).encode("utf-8")
                    self._emit(line_out)
            except Exception as e:
                logging.error(e)
                logging.error(line)
                continue

    def reducer(self):
        """
        方法需要在继承类上重写
        :return:
        """
        for line in self._reducer_in():
            try:
                line = line.strip()
                kv = line.split("\t")
                self._emit(kv[0], kv[1])
            except Exception as e:
                continue

    def out_put(self):
        """
        本地测试时使用
        :return:
        """
        fp = open(self.OUTPUT_FILE, "w+")
        # fp.write("\n".join(self.collection))
        for line in self.collection:
            try:
                fp.write(line)
                fp.write("\n")
            except Exception as e:
                logging.error(e)
        fp.close()

    def test(self):
        if not self.TEST:
            print "not test mode"
            return

        print "begin to mapper"
        self.mapper()
        print "shuffle and sort"
        self.mapper_out = self.collection
        self.collection = []
        self.mapper_out.sort()
        print "begin to reduce"
        self.reducer()
        print "output data"
        self.out_put()
        print "all over, result is in: %s" % self.OUTPUT_FILE
        print "valid_lines: %s" % self.valid_lines
        print "not_match_lines: %s" % self.not_match_lines
        print "error_lines: %s" % self.error_lines


class BaseMapredLocal(BaseMapred):

    def __init__(self, date):
        super(BaseMapredLocal, self).__init__()
        self.date = date
        self.default_dir = "/app/ps/spider/wdmqa/kgdc/"
        self.product = self.get_product_name()
        self.input_dir = os.path.join(self.default_dir, self.date, self.product, "input")
        self.output_dir = os.path.join(self.default_dir, self.date, self.product, "output")

    def get_product_name(self):
        """
        获取产品名字，本质就是不带后缀的文件名
        :return:
        """
        product = __file__
        product = os.path.basename(product)
        product, suffix = os.path.splitext(product)
        return product

    def set_up(self):
        """
        环境准备，包括拷贝数据到本集群
        :return:
        """
        # 是否包含日期信息
        if "%s" in self.SOURCE:
            self.SOURCE = self.SOURCE % self.date
        # 是否distcp
        if self.SOURCE.startswith("hdfs://"):
            current_dir = os.path.split(os.path.abspath(__file__))[0]
            cmd = "cd %s; sh ./run_distcp.sh %s %s %s" % (
                current_dir,
                conf.HADOOP_BIN,
                self.SOURCE,
                self.input_dir)
            logging.info(cmd)
            os.system(cmd)

    def mapred(self):
        pass

    def clear_down(self):
        pass

    def run(self):
        self.set_up()
        self.mapred()
        self.clear_down()


def test(product):
    """
    测试程序本地部分
    :param product:
    :return:
    """
    date = "20170103"
    a = BaseMapredLocal(date)
    a.run()


def test_mapred(product):
    """
    测试程序远程部分
    :param product:
    :return:
    """
    date = "20170103"
    module = import_module("midpage.products_mapred.%s" % product)

    class TestProduct(module.Mapred):
        TEST = True
        SOURCE = "/home/work/temp/nj02-ps-ae-app9.nj02.baidu.com-frontend_access.log.2017010312.669"
        OUTPUT_FILE = "/home/work/temp/nj02.output"
    a = TestProduct(date)
    a.test()


def test_filter():
    line = '117.136.81.248 - - [03/Jan/2017:12:59:59 +0800] "GET /static/asset/dep/asset/img/play-icon-m.png HTTP/1.0" 200 '\
           '4148 "http://tiyu.baidu.com/detail?name=TkJBIzIwMTctMDEtMDMj5o6Y6YeRdnPli4flo6s%3D" "Hm_lpvt_dfcd11bffa2fa6ca44'\
           'ab9ae5fe0b4a7f=1483419596; Hm_lvt_dfcd11bffa2fa6ca44ab9ae5fe0b4a7f=1483419201,1483419429,1483419493,1483419596;'\
           'BAIDUID=541937CDCDCC8D82C285CBA87473BB6A:FG=1; BIDUPSID=779B3663B5E318A39F0924166778C64F; H_WISE_SIDS=109815_1'\
           '02570_100614_100042_114039_102432_110567_100100_113960_107799_113932_107316_111551_114207_112106_114000_111462_'\
           '113568_111928_111366_114197_113566_112135_110031_114132_114095_110086; PSINO=1" "Mozilla/5.0 (iPhone; CPU iPhon'\
           'e OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Mobile/11D257 baiduboxapp/0_0.6.0.6_enohpi_0'\
           '69_046/2.1.7_1C2%254enohPi/1099a/2C920D743047AF5F06E6A5AF65467F563B65083BDFROACMILLB/1" rt=0.003 11398553596525'\
           '209892 10.46.142.64 10.44.9.43 tiyu.baidu.com "117.136.81.248" ps appEngine - 1483419599.844'
    date = "20170103"
    a = BaseMapredLocal(date)
    match = a.FILTER.match(line)
    if match:
        print match.groupdict()
    else:
        print "not match"
