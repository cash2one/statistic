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
import sys
import copy
import json
import urlparse
import logging
# 第三方库

# 自有库
import common


class BaseMapred(object):
    """
    执行hadoop任务的基类
    类里所有接口都是用于集群上执行。
    同时有构造测试流程
    """
    ###################################
    # 本地测试开关， 测试方法，在基类上继承一个新类，定义Test为True即可
    TEST = False
    ###################################
    # 指定日志来源。可以nanlin本地集群，可以为其他集群。其他集群会先执行distcp
    SOURCE = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310" \
             "/app/dt/minos/3/textlog/www/wise_tiyu_access/70025011/%s/1200/"
    ###################################
    # 本地测试时，存储结果的位置
    OUTPUT_FILE = ""
    ###################################
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^(?P<client_ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status>[0-9]+) ([0-9]+) "
                        r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" "
                        r"\"(?P<user_agent>.*)\" rt=(?P<cost_time>[0-9\.]+) [0-9]* "
                        r"([0-9\.]+) ([0-9\.]+) (.*) "
                        r"\".*\" ps appEngine - (?P<msec>[0-9\.]+)$")
    ###################################
    # cookie中搜索百度uid匹配规则
    BAIDUID_REG = re.compile(r"BAIDUID=(?P<id>.+?):(.*=\d*)(;|$)")
    ###################################
    # 设备匹配规则
    IOS_REG = re.compile(r"(?i)Mac OS X")
    ANDROID_REG = re.compile(r"(?i)android")
    NA_REG = re.compile(r"(xiaodurobot|dueriosapp|duerandroidapp)")
    MB_REG = re.compile(r"baiduboxapp")
    ###################################
    # 指标配置
    # example，具体类需要复写
    index_map = {
        # 每一项即为一个指标，KEY就是指标名，中文务必加 u
        u"pv": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "count",
            "reducer": "merge_index",
            "config": {},
            # 该指标对应的维度信息，在下面定义
            "group_name": "default_groups"
        },
        # u"图片点击数": {
        #     "query": {
        #         "query.cat": "dumi_meishi",
        #         "query.act": "b_click_focusimg",
        #     },
        #     "type": "count",
        # },
        # u"图片点击率": {
        #     "numerator": u"图片点击数",
        #     "denominator": u"pv",
        #     "type": "percent",
        # },
        # u"多个指标相加": {
        #     "index": ["pv", "图片点击数"],
        #     "type": "add",
        # },
        # u"插值": {
        #     "minuend": u"被减数",
        #     "subtrahend": u"减数",
        #     "type": "sub",
        # },
        # u"uv": {
        #     "query": {},
        #     "distinct_key": "baiduid",
        #     "type": "distinct_count",
        # },
        # u"map reduce计算": {
        #     "query": {},
        #     "map": "function () {}",
        #     "reduce": "function (key, values) {}",
        #     "local": "<python function>",
        #     "type": "map_reduce",
        # },
        # u"本地计算": {
        #     "query": {},
        #     "fields": {
        #         "timestamp": 1,
        #         "baiduid": 1,
        #     },
        #     "local": "<python function>",
        #     "type": "output",
        # },
        # u"求和": {
        #     "query": {},
        #     "field": "query.extend.card_num",
        #     "type": "sum",
        # },
        # u"求平均值": {
        #     "query": {},
        #     "field": "query.extend.card_num",
        #     "type": "avg",
        # },
    }
    ###################################
    # 用户画像配置
    ###################################
    # 用户路径分析配置
    ###################################
    # 维度汇总配置

    # 配置指标分组的多级结构
    # 如 OS(ios/android)->client(NA/MB)->指标
    # 那么此处list就写上2组。如果类型为file，表示最终会按照该维度进行文件分组
    # 最终目的，是根据此配置，将指标拉平为：
    # {os: ios, client: NA, 指标：指标值}
    # {os: ios, client: MB, 指标：指标值}
    # {os: android, client: NA, 指标：指标值}
    # {os: android, client: MB, 指标：指标值}

    # 在指标配置中需要使用到的group配置，都必须写在这个list中，程序需要进行展开处理
    groups = ["default_groups"]
    # groups中的2个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # key：表示在数据中，该维度对应的key名字。
    # 该配置 是对下面每个维度的一个汇总。可以存在多个维度汇总，名字自定义。
    default_groups = [{
        "key": "os",
        "attribute": "os_group",
    }, {
        "attribute": "client_group",
        "key": "client",
    }, {
        "attribute": "type_group",
        "key": "type",
    }]
    ###################################
    # 具体每个维度配置
    # 规定了该维度下，value的取值有哪些选项，名称自定义，上面的维度汇总配置引用
    # name：value取值项
    # query：匹配中该维度的查询条件
    os_group = [{
        "name": "ios",
        "query": {"os": "ios"},
    }, {
        "name": "android",
        "query": {"os": "android"},
    }, {
        "name": "other",
        "query": {"os": "other"},
    }]

    client_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "NA",
        "query": {"client": "NA"},
    }, {
        "name": "MB",
        "query": {"client": "MB"},
    }, {
        "name": "other",
        "query": {"os": "other"},
    }]

    type_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "detail",
        "query": {"url": "/detail"},
    }, {
        "name": "category",
        "query": {"url": "/category"},
    }, {
        "name": "news",
        "query": {"url": "/news"},
    }, {
        "name": "videodetail",
        "query": {"url": "/videodetail"},
    }]
    ###################################
    # 结果处理配置

    def __init__(self):
        """
        初始化
        :param mapred:
        :param source:
        :return:
        """
        # 所有group配置的展开合集
        self.groups_expand = dict()
        self.valid_lines = 0
        self.not_match_lines = 0
        self.error_lines = 0
        self.emit_lines = 0

        if self.TEST:
            self.collection = []
            self.mapper_out = None
            self._mapper_in = self._mapper_in_test
            self._reducer_in = self._reduce_in_test
            self._emit = self._emit_test

    def _iter_groups(self, groups, layer=0):
        """
        根据groups中的配置，将配置拉平
        如 OS(ios/android)->client(NA/MB)->指标
        那么此处list就写上2组。如果类型为file，表示最终会按照该维度进行文件分组
        最终目的，是根据此配置，将指标拉平为：
        [
        [{ios配置}，{NA配置}],
        [{ios配置}，{MB配置}],
        [{android配置}，{NA配置}],
        [{android配置}，{MB配置}],
        ]
        :param groups:
        :param layer:
        :return:
        """
        total_layer = len(groups) - 1
        group_info = groups[layer]
        group_queries = getattr(self, group_info["attribute"])
        if len(group_queries) == 0:
            raise Exception("[ERROR]group can't be empty:%s" % group_info["attribute"])
        for g in group_queries:
            ret = [copy.deepcopy(g)]
            # added by xulei12@baidu.com 2016.6.20 增加一个key字段，推送到mongo用
            for item in ret:
                item["key"] = group_info["key"]
            # add ended
            if layer == total_layer:
                yield ret
            else:
                for ext in self._iter_groups(groups, layer + 1):
                    tmp = copy.deepcopy(ret)
                    yield tmp + ext

    def _get_query_and_keys(self, groups):
        """
        获取指标限定条件与group的对应关系。
        group的配置中，支持os.os,这样的写法。会自动展开。举例格式如下:
        {'keys': {'type': 'total', 'client': 'total', 'os': 'total'}, 'query': {}}
        {'keys': {'type': 'detail', 'client': 'total', 'os': 'total'}, 'query': {'url': '/detail'}}
        {'keys': {'type': 'category', 'client': 'total', 'os': 'total'}, 'query': {'url': '/category'}}
        {'keys': {'type': 'news', 'client': 'total', 'os': 'total'}, 'query': {'url': '/news'}}
        {'keys': {'type': 'videodetail', 'client': 'total', 'os': 'total'}, 'query': {'url': '/videodetail'}}
        {'keys': {'type': 'total', 'client': 'NA', 'os': 'total'}, 'query': {'os': {'client': 'NA'}}}
        :param groups:
        :return:
        """
        for group in self._iter_groups(groups):
            query = dict()
            keys = dict()
            for g in group:
                query.update(g["query"])
                keys.update({g["key"]: g["name"]})
            yield {"query": query, "keys": keys}

    def expand_query(self, query):
        """
        将{"query.cat": "a", "query.mat": "b"}这样写法的query扩展成通用json
        {"query": {"cat": "a", "mat": "b"}}
        :param query:
        :return:
        """
        expand_query = dict()
        for one_query in query:
            items = one_query.split(".")
            if len(items) > 1:
                temp_query = expand_query
                for item in items[:-1]:
                    temp_query.setdefault(item, dict())
                    temp_query = temp_query[item]
                temp_query[items[-1]] = query[one_query]
            else:
                expand_query[one_query] = query[one_query]
        return expand_query

    def expand_index(self, index, values, group_key):
        """
        将
        {
            "android":
                {"na": 1}
        }
        拉平为
        {"os": "android", "agent": "na", "@index": "pv", "@value": 1}

        :param index: 指标名称。如 pv
        :param values: 带深度的指标值。
        :param group_key: 指标维度各级别名称。如：["os", "agent"]
        :return:
        """
        if len(group_key) == 0:
            yield {"@value": values, "@index": index}

        if len(group_key) == 1:
            for key, value in values.items():
                yield {"@value": value, "@index": index, group_key[0]: key}
        else:
            # 此处有坑。必须复制一下
            temp_key = copy.deepcopy(group_key)
            group_name = temp_key.pop(0)
            for key, item in values.items():
                for one_record in self.expand_index(index, item, temp_key):
                    one_record[group_name] = key
                    yield one_record

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
            self.collection.append(key)

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
            raise Exception("request error")
        request = urlparse.urlparse(request)
        line['url'] = request.path
        if len(line['url']) > 1024:
            raise Exception("url too long")
        line['query'] = self.parse_query(request.query)

        for field in line['query']:
            if field.endswith('_num'):
                try:
                    line['query'][field] = int(line['query'][field])
                except:
                    raise Exception("end with _num should be num")
        if 'duration' in line['query']:
            try:
                line['query']['duration'] = float(line['query']['duration'])
            except:
                raise Exception("duration error")
        if 'extend' in line['query']:
            try:
                line['query']['extend'] = json.loads(line['query']['extend'])
            except:
                logging.exception('')
                raise Exception("extend should be a json")
            for key in line['query']['extend']:
                if key.endswith('_num'):
                    try:
                        line['query']['extend'][key] = float(line['query']['extend'][key])
                    except:
                        raise Exception("query extend error")

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
                raise Exception("query format error")
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

    def _count(self, line, index_item):
        """
        简单统计
        "uv": {
            "query": {},
            "type": "count",
            "group_key": ["os", "client", "type"]
            "group": yield对象
        }
        yield对象的一个元素举例
        {'keys': {'type': 'total', 'client': 'total', 'os': 'total'}, 'query': {}}
        :param line: 要分析的行数
        :param index_item: self.index_map 中某个指标的全部配置
        :return:
        """
        for group in index_item["group"]:
            if common.json_equal(line, group["query"]):
                # 符合就逐级检查，最后一级增加计数
                if len(index_item["group_key"]) == 0:
                    index_item.setdefault("@value", 0)
                    index_item["@value"] += 1
                else:
                    index_item.setdefault("@value", dict())
                    temp_dict = index_item["@value"]
                    for key in index_item["group_key"][:-1]:
                        temp_dict.setdefault(group["keys"][key], dict())
                        temp_dict = temp_dict[group["keys"][key]]
                    temp_dict.setdefault(group["keys"][index_item["group_key"][-1]], 0)
                    temp_dict[group["keys"][index_item["group_key"][-1]]] += 1

    def recurse_merge(self, target, source):
        """
        递归合并两个词典。要求两个词典的层数要一样。专门用来合并不同mapper结果的指标统计值。
        :param target: 存储结果位置
        :param source: 合入到target
        :return:
        """
        # 判断元素的类别。如果是词典，则继续合并内部。
        if type(source[source.keys()[0]]) == dict:
            for key, value in source.items():
                if key not in target:
                    target.setdefault(key, dict())
                self.recurse_merge(target[key], value)
        # 若不是，则直接合并。
        else:
            for key, value in source.items():
                target.setdefault(key, 0)
                target[key] += value

    def _merge_index(self, line, index_item):
        """
        "uv": {
            "query": {},
            "type": "count",
            "group_key": ["os", "client", "type"]
            "group": yield对象
        }
        :param line: 字符串输入
        :param index_item:
        :return:
        """
        try:
            line = json.loads(line)
        except:
            logging.error("json error: %s" % line)
            return
        if "@value" not in index_item or not index_item["@value"]:
            index_item["@value"] = line
        else:
            try:
                # 不分维度的情况，line会是一个自然数字
                if len(index_item["group_key"]) == 0:
                    logging.error("no group")
                    index_item["@value"] += line
                else:
                    logging.error("has group, go to merge")
                    logging.error(index_item["@value"])
                    self.recurse_merge(index_item["@value"], line)
            except:
                logging.exception("calculate error")
                logging.error("value=%s\nline=%s" % (index_item["@value"], line))

    def get_function(self, name):
        """
        根据配置获取处理函数
        :param name: 处理方法名字
        :return:
        """
        name = "_" + name
        try:
            func = getattr(self, name)
        except:
            func = None
        return func

    def calculate_index_mapper(self, line):
        """

        :param line: json格式
        :return:
        """
        for index, item in self.index_map.items():
            if item["mapper"]:
                func = self.get_function(item["mapper"])
                func(line, item)

    def calculate_index_reducer(self, line):
        """

        :param line: 原始输入字符串
        :return:
        """
        kv = line.split("\t")
        index = kv[0]
        value = kv[1]
        if index in self.index_map:
            func = self.get_function(self.index_map[index]["reducer"])
            func(value, self.index_map[index])

    def get_group_key(self):
        """
        根据group配置生成key顺序。比如本例子
        default_groups = [{
            "key": "os",
            "attribute": "os_group",
        }, {
            "attribute": "client_group",
            "key": "client",
        }, {
            "attribute": "type_group",
            "key": "type",
        }]
        生成的数据为
        ["os", "client", "type"]

        :return:
        """
        for index, item in self.index_map.items():
            group = getattr(self, item["group_name"])
            item["group_key"] = [level["key"] for level in group]

    def _mapper_set_up(self):
        # 记录维度的顺序。方便后面统计
        self.get_group_key()
        # 对所有分组配置进行拆分
        for item in self.groups:
            group = getattr(self, item)
            self.groups_expand[item] = self._get_query_and_keys(group)
        for index in self.index_map:
            group_name = self.index_map[index]["group_name"]
            self.index_map[index]["group"] = []
            for one_group in self.groups_expand[group_name]:
                one_group["query"].update(self.index_map[index]["query"])
                one_group["query"] = self.expand_query(one_group["query"])
                one_group["keys"]["@index"] = index
                self.index_map[index][u"group"].append(one_group)

    def _mapper(self):
        """
        方法可能需要在继承类上重写
        :return:
        """
        for line in self._mapper_in():
            try:
                line = line.strip()
                line = self.analysis_line(line)
                if line:
                    self.calculate_index_mapper(line)
            except Exception as e:
                logging.error(e)
                logging.error(line)
                continue

        for index, item in self.index_map.items():
            # 不一定每一个mapper任务都能跑出每个指标的结果
            if "@value" not in item:
                continue
            if type(item["@value"]) == list:
                for line in item["@value"]:
                    self.emit_lines += 1
                    if type(line) == dict:
                        self._emit(index.encode("utf-8"),
                                   json.dumps(line, ensure_ascii=False).encode("utf-8"))
                    else:
                        self._emit(index.encode("utf-8"), line)
            elif type(item["@value"]) == dict:
                self.emit_lines += 1
                self._emit(index.encode("utf-8"),
                           json.dumps(item["@value"], ensure_ascii=False).encode("utf-8"))
            else:
                self.emit_lines += 1
                self._emit(index.encode("utf-8"), item["@value"])

    def _mapper_clear_down(self):
        """
        mapper完成后的环境清理，避免可能的数据干扰（特别是本地测试）
        :return:
        """
        self.groups_expand = dict()
        self.valid_lines = 0
        self.not_match_lines = 0
        self.error_lines = 0
        self.emit_lines = 0
        for index, item in self.index_map.items():
            try:
                del item["group"]
                del item["@value"]
            except:
                pass

    def mapper(self):
        """
        :return:
        """
        logging.error("self.Test = %s" % self.TEST)
        self._mapper_set_up()
        self._mapper()
        logging.error("valid_lines: %s" % self.valid_lines)
        logging.error("not_match_lines: %s" % self.not_match_lines)
        logging.error("error_lines: %s" % self.error_lines)
        logging.error("emit_lines: %s" % self.emit_lines)
        self._mapper_clear_down()

    def _reducer_set_up(self):
        """
        reducer准备工作
        :return:
        """
        self.get_group_key()

    def _reducer(self):
        """
        方法需要在继承类上重写
        :return:
        """
        for line in self._reducer_in():
            try:
                line = line.strip()
                self.valid_lines += 1
                self.calculate_index_reducer(line)
            except Exception as e:
                self.error_lines += 1
                continue
        for index, item in self.index_map.items():
            # 不一定每个指标在每个mapper过程都能计算出结果
            if "@value" not in item:
                continue
            logging.error("@value=%s" % item["@value"])
            for record in self.expand_index(index, item["@value"], item["group_key"]):
                self.emit_lines += 1
                self._emit(json.dumps(record, ensure_ascii=False).encode("utf-8"))

    def reducer(self):
        """

        :return:
        """
        logging.error("self.Test = %s" % self.TEST)
        self._reducer_set_up()
        self._reducer()
        logging.error("valid_lines: %s" % self.valid_lines)
        logging.error("not_match_lines: %s" % self.not_match_lines)
        logging.error("error_lines: %s" % self.error_lines)
        logging.error("emit_lines: %s" % self.emit_lines)

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
        """
        本地测试方法
        :return:
        """
        if not self.TEST:
            print "not test mode"
            return

        print "begin to mapper"
        self.mapper()
        print "shuffle and sort"
        self.mapper_out = self.collection
        self.collection = []
        self.valid_lines = self.not_match_lines = self.error_lines = 0
        self.mapper_out.sort()
        print "begin to reduce"
        self.reducer()
        print "output data"
        self.out_put()
        print "all over, result is in: %s" % self.OUTPUT_FILE
        print "valid_lines: %s" % self.valid_lines
        print "not_match_lines: %s" % self.not_match_lines
        print "error_lines: %s" % self.error_lines

    def test_iter_group(self):
        """

        :return:
        """
        for group in self._get_query_and_keys(self.default_groups):
            print group

    def test_get_function(self):
        """

        :return:
        """
        print self.get_function("count")
        print self.get_function("abc")

    def test_merge(self):
        """

        :return:
        """
        target = {
            "android": {
                "total": {
                    "category": 1,
                    "videodetail": 7
                },
                "other": {
                    "total": 5,
                    "detail": 1
                },
                "MB": {
                    "category": 1,
                    "news": 4,
                    "total": 213,
                    "detail": 18,
                    "videodetail": 1
                }
            }
        }
        target = {}
        source = {
            "other": {
                "total": {
                    "total": 5,
                    "detail": 1
                },
                "other": {
                    "total": 5,
                    "detail": 1
                }
            },
            "android": {
                "NA": {"total": 1},
                "total": {
                    "category": 1,
                    "detail": 20
                }
            }
        }
        self.recurse_merge(target, source)
        print target

    def test_expand_index(self):
        """

        :return:
        """
        values = {
            "android": {
                "total": {
                    "category": 1,
                    "videodetail": 7
                },
                "other": {
                    "total": 5,
                    "detail": 1
                },
                "MB": {
                    "category": 1,
                    "news": 4,
                    "total": 213,
                    "detail": 18,
                    "videodetail": 1
                }
            },
            "other": {
                "MB": {
                    "category": 1,
                    "news": 4,
                }
            }
        }
        self.get_group_key()
        for item in self.expand_index("pv", values, self.index_map["pv"]["group_key"]):
            print item


def test():
    """
    本地测试
    :return:
    """
    a = BaseMapred()
    a.test_merge()
    # a.test()