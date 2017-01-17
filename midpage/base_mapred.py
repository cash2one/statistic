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
import copy
import json
import urlparse
import logging
# 第三方库

# 自有库
import common
import hadoop


class BaseMapred(hadoop.Hadoop):
    """
    执行hadoop任务的基类
    类里所有接口都是用于集群上执行。
    同时有构造测试流程
    """
    ###################################
    # 指定日志来源。可以nanlin本地集群，可以为其他集群。其他集群会先执行distcp
    SOURCE = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310" \
             "/app/dt/minos/3/textlog/www/wise_tiyu_access/70025011/%s/1200/"
    ###################################
    # 过滤配置，符合该正则的认为是合法的日志
    FILTER = re.compile(r"^(?P<client_ip>[0-9\.]+) (.*) (.*) (?P<time>\[.+\]) "
                        r"\"(?P<request>.*)\" (?P<status>[0-3][0-9]+) ([0-9]+) "
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
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "default_groups"
        },
        u"uv": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "distinct_count",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "mapper": "BAIDUID",
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "default_groups"
        },
        u"uid_list": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "distinct",
            "reducer": "distinct",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "mapper": "BAIDUID",
                "reducer": "",
                "local": "%s_uid.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": ""
        },
        u"user_path": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "user_path_mapper",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "user_path_local",
            # 以上三个过程都是针对mapred操作特点，对每一行进行处理。
            # gather是搜集所有信息后，对汇总信息进行处理。诸如指标之间的运算，汇总后处理等。
            "gather": "user_path_gather",
            "config": {
                "gather": {
                    "target": ["/live", "/category", "/detail", "/player"],
                    "file": "%s_user_path.txt"
                }
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "user_path_groups"
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
    groups = ["default_groups", "pv_groups"]
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
    pv_groups = [{
        "key": "page",
        "attribute": "page_group"
    }, {
        "key": "tab",
        "attribute": "tab_group"
    }]
    user_path_groups = [{
        "key": "url",
    }, {
        "key": "referr",
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

    page_group = [{
        "name": "total",
        "query": {}
    }, {
        "name": "live",
        "query": {"page": "live"}
    }, {
        "name": "category",
        "query": {"page": "category"}
    }, {
        "name": "detail",
        "query": {"page": "detail"}
    }, {
        "name": "player",
        "query": {"page": "player"}
    }]

    tab_group = [{
        "name": "total",
        "query": {},
    }]
    ###################################
    # 结果处理配置

    def __init__(self, **kwargs):
        """
        初始化
        :param mapred:
        :param source:
        :return:
        """
        super(BaseMapred, self).__init__(**kwargs)
        # 所有group配置的展开合集
        self.groups_expand = dict()
        self.valid_lines = 0
        self.not_match_lines = 0
        self.error_lines = 0
        self.emit_lines = 0

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
        if "attribute" not in group_info:
            return
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

    def contract_index(self, ret, values, group_key):
        """
        与expand操作相反
        将
        {"os": "android", "agent": "na", "@index": "pv", "@value": 1}
        压缩为
        {
            "android":
                {"na": 1}
        }
        如果values本身已经有一些值，则与之合并，并相加。
        :param ret: 带深度的指标值。
        :param values: 需要转换的指标。
        :param group_key: 指标维度各级别名称。如：["os", "agent"]
        :return:
        """
        # 符合就逐级检查，最后一级增加计数
        if len(group_key) == 0:
            if ret:
                ret += values["@value"]
            else:
                ret = values["@value"]
        else:
            if not ret:
                ret = dict()
            temp_dict = ret
            for key in group_key[:-1]:
                temp_dict.setdefault(values[key], dict())
                temp_dict = temp_dict[values[key]]
            temp_dict.setdefault(values[group_key[-1]], 0)
            temp_dict[values[group_key[-1]]] += values["@value"]
        return ret

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
                logging.exception(line['query']['extend'])
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
            line["BAIDUID"] = bdid.group("id")

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

    def format_line(self, line):
        """
        对部分字段进行数据类型转换
        :param line:
        :return:
        """
        if "status" in line:
            line["status"] = int(line["status"])

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
        self.format_line(line)

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
        用于mapper过程
        简单统计
        "uv": {
            "query": {},
            "type": "count",
            "group_key": ["os", "client", "type"]
            "group": yield对象
        }
        yield对象的一个元素举例
        {'keys': {'type': 'total', 'client': 'total', 'os': 'total'}, 'query': {}}
        :param line: 要分析的行， 已经是json格式
        :param index_item: self.index_map 中某个指标的全部配置
        :return:
        """
        for group in index_item["group"]:
            if common.json_equal(line, group["query"]):
                temp_value = copy.deepcopy(group["keys"])
                temp_value["@value"] = 1
                index_item.setdefault("@value", None)
                index_item["@value"] = self.contract_index(index_item["@value"],
                                                           temp_value,
                                                           index_item["group_key"])

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

    def _merge_index(self, line, index_item, config=None):
        """
        用于reduce过程
        "uv": {
            "query": {},
            "type": "count",
            "group_key": ["os", "client", "type"]
            "group": yield对象
        }
        :param line: 字符串输入
        :param index_item:
        :param config: 可为空。保证接口统一
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

    def _distinct(self, line, index_item, config=None):
        """
        按照配置给定的字段
        mapper和reducer过程都可能调用
        :param line:
        :param index_item:
        :param config: 用以去重的key
        :return:
        """
        if config:
            key = line.get(config)
        else:
            key = line
        index_item.setdefault("@value", set())
        if key:
            index_item["@value"].add(key)

    def _distinct_count(self, line, index_item, key=None):
        """
        用于mapper过程
        简单统计
        "uv": {
            "query": {},
            "type": "count",
            "group_key": ["os", "client", "type"]
            "group": yield对象
        }
        yield对象的一个元素举例
        {'keys': {'type': 'total', 'client': 'total', 'os': 'total'}, 'query': {}}
        :param line: 要分析的行， 已经是json格式
        :param index_item: self.index_map 中某个指标的全部配置
        :param key: 用于distinct的key
        :return:
        """
        if not key:
            return
        # 用来去重用的标记
        distinct_value = line.get(key)
        if not distinct_value:
            return
        for group in index_item["group"]:
            # 先检查是否匹配
            if common.json_equal(line, group["query"]):
                # 符合就逐级检查，最后一级增加计数
                if len(index_item["group_key"]) == 0:
                    index_item.setdefault("@value", 0)
                    index_item.setdefault("distinct", set())
                    # 检查是否统计过， 使用set来去重。
                    if distinct_value not in index_item["distinct"]:
                        index_item["distinct"].add(distinct_value)
                        index_item["@value"] += 1
                else:
                    # 如果带维度，先循环至最内层。使用"distinct"来保存去重的列表
                    index_item.setdefault("@value", dict())
                    index_item.setdefault("distinct", dict())
                    temp_dict = index_item["@value"]
                    temp_distinct = index_item["distinct"]
                    for key in index_item["group_key"][:-1]:
                        temp_dict.setdefault(group["keys"][key], dict())
                        temp_distinct.setdefault(group["keys"][key], dict())
                        temp_dict = temp_dict[group["keys"][key]]
                        temp_distinct = temp_distinct[group["keys"][key]]
                    # 获取到最内层
                    temp_dict.setdefault(group["keys"][index_item["group_key"][-1]], 0)
                    temp_distinct.setdefault(group["keys"][index_item["group_key"][-1]], set())
                    temp_distinct = temp_distinct[group["keys"][index_item["group_key"][-1]]]
                    # 检查是否统计过
                    if distinct_value not in temp_distinct:
                        temp_distinct.add(distinct_value)
                        temp_dict[group["keys"][index_item["group_key"][-1]]] += 1

    def _user_path_mapper(self, line, index_item):
        """
        用户路径分析
        :param line:
        :param index_item:
        :return:
        """
        referr = line["referr"]
        url = line["url"]
        if "." in referr or "." in url:
            return
        if not referr:
            referr = "-"
        temp_value = {"referr": referr, "url": url, "@value": 1}
        index_item.setdefault("@value", dict())
        index_item["@value"] = self.contract_index(index_item["@value"],
                                                   temp_value,
                                                   index_item["group_key"])

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
                if "mapper" in item["config"]:
                    config = item["config"]["mapper"]
                    func(line, item, config)
                else:
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
            if "reducer" in self.index_map[index]["config"]:
                config = self.index_map[index]["config"]["reducer"]
                func(value, self.index_map[index], config)
            else:
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
            group_name = item.get("group_name")
            if group_name:
                group = getattr(self, item["group_name"])
                if group:
                    item["group_key"] = [level["key"] for level in group]

    def analysis(self):
        """
        用于第一步的mapreduce任务的map。用于分析，并按照BAIDUID进行分发，便于统计uv
        :return:
        """
        for line in self._mapper_in():
            try:
                line = line.strip()
                line = self.analysis_line(line)
                if line:
                    if "BAIDUID" in line:
                        key = line["BAIDUID"]
                    else:
                        key = "0" * 32
                    self._emit(key,
                               json.dumps(line, ensure_ascii=False).encode("utf-8"))
            except Exception as e:
                logging.error(e)
                logging.error(line)
                continue

        logging.error("valid_lines: %s" % self.valid_lines)
        logging.error("not_match_lines: %s" % self.not_match_lines)
        logging.error("error_lines: %s" % self.error_lines)
        logging.error("emit_lines: %s" % self.emit_lines)

    def _mapper_set_up(self):
        # 记录维度的顺序。方便后面统计
        self.get_group_key()
        # 对所有分组配置进行拆分
        for item in self.groups:
            group = getattr(self, item)
            self.groups_expand[item] = list(self._get_query_and_keys(group))
        for index in self.index_map:
            group_name = self.index_map[index].get("group_name")
            self.index_map[index]["group"] = []
            if group_name:
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
                kv = line.split("\t")
                line = json.loads(kv[1])
                self.valid_lines += 1
                self.calculate_index_mapper(line)
            except Exception as e:
                self.error_lines += 1
                logging.error(e)
                logging.error(line)
                continue
        for index, item in self.index_map.items():
            # 不一定每一个mapper任务都能跑出每个指标的结果
            if "@value" not in item:
                continue
            if type(item["@value"]) == set:
                item["@value"] = list(item["@value"])
            if type(item["@value"]) == list:
                item["@value"].sort()
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
                del item["distinct"]
            except:
                pass

    def mapper(self):
        """
        :return:
        """
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

            if type(item["@value"]) == set:
                item["@value"] = list(item["@value"])
            if type(item["@value"]) == list:
                item["@value"].sort()
                for line in item["@value"]:
                    self.emit_lines += 1
                    if type(line) == dict:
                        self._emit(index.encode("utf-8"),
                                   json.dumps(line, ensure_ascii=False).encode("utf-8"))
                    else:
                        self._emit(index.encode("utf-8"), line)
            elif type(item["@value"]) == dict:
                for record in self.expand_index(index, item["@value"], item["group_key"]):
                    self.emit_lines += 1
                    self._emit(index.encode("utf-8"),
                               json.dumps(record, ensure_ascii=False).encode("utf-8"))
            else:
                self.emit_lines += 1
                self._emit(index.encode("utf-8"), item["@value"])

    def reducer(self):
        """

        :return:
        """
        self._reducer_set_up()
        self._reducer()
        logging.error("valid_lines: %s" % self.valid_lines)
        logging.error("not_match_lines: %s" % self.not_match_lines)
        logging.error("error_lines: %s" % self.error_lines)
        logging.error("emit_lines: %s" % self.emit_lines)

    def test(self):
        """
        本地测试方法
        :return:
        """
        if not self.test_mode:
            print "not test mode"
            return
        if not self.in_fine:
            logging.error("please set parameter: in_file")
            return
        if not self.out_file:
            logging.error("please set parameter: out_file")
            return

        print "********first job********"
        print "begin to mapper"
        self.analysis()
        output_file = self.out_file
        self.out_file = output_file + ".temp"
        print "begin to reducer"
        self.collection.sort()
        self._out_put()
        print "first over, result is in: %s" % self.out_file
        print "valid_lines: %s" % self.valid_lines
        print "not_match_lines: %s" % self.not_match_lines
        print "error_lines: %s" % self.error_lines
        self.valid_lines = 0
        self.not_match_lines = 0
        self.error_lines = 0
        self.collection = []
        self.in_fine = self.out_file
        self.out_file = output_file
        print "*********second job*******"
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
        self._out_put()
        print "all over, result is in: %s" % self.out_file
        print "valid_lines: %s" % self.valid_lines
        print "not_match_lines: %s" % self.not_match_lines
        print "error_lines: %s" % self.error_lines

    def test_iter_group(self):
        """

        :return:
        """
        for group in self._get_query_and_keys(self.pv_groups):
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
    a.test_iter_group()
    # a.test()
