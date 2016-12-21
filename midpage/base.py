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
import os
import re
import copy
import json
import types
import logging
# 第三方库
from bson.code import Code
# 自有库
import lib.tools
from conf import conf
from midpage import midpagedb


class MidpageProduct(object):
    def __init__(self, date):
        self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()

    def statist(self):
        raise NotImplementedError()

    def save_result(self, result):
        raise NotImplementedError()

    def run(self):
        self.save_result(self.statist())


class CRMMidpageProduct(object):
    """
    各业务Product类，继承的基类，基本功能在此实现
    """
    default_query = {}

    # example，具体类需要复写
    index_map = {
        # 每一项即为一个指标，KEY就是指标名
        u"pv": {
            # query为计算指标需要用的参数或者字段
            "query": {
                "query.cat": "dumi_meishi",
                "query.act": "pv",
            },
            # type为计算指标的方法，比如count对应到的方法为 _count_statistic()
            "type": "count",
        },
        u"图片点击数": {
            "query": {
                "query.cat": "dumi_meishi",
                "query.act": "b_click_focusimg",
            },
            "type": "count",
        },
        u"图片点击率": {
            "numerator": u"图片点击数",
            "denominator": u"pv",
            "type": "percent",
        },
        u"多个指标相加": {
            "index": ["pv", "图片点击数"],
            "type": "add",
        },
        u"插值": {
            "minuend": u"被减数",
            "subtrahend": u"减数",
            "type": "sub",
        },
        u"uv": {
            "query": {},
            "distinct_key": "baiduid",
            "type": "distinct_count",
        },
        u"map reduce计算": {
            "query": {},
            "map": "function () {}",
            "reduce": "function (key, values) {}",
            "local": "<python function>",
            "type": "map_reduce",
        },
        u"本地计算": {
            "query": {},
            "fields": {
                "timestamp": 1,
                "baiduid": 1,
            },
            "local": "<python function>",
            "type": "output",
        },
        u"求和": {
            "query": {},
            "field": "query.extend.card_num",
            "type": "sum",
        },
        u"求平均值": {
            "query": {},
            "field": "query.extend.card_num",
            "type": "avg",
        },
    }

    # 配置指标分组的多级结构
    # 如 OS(ios/android)->client(NA/MB)->指标
    # 那么此处list就写上2组。如果类型为file，表示最终会按照该维度进行文件分组
    # 最终目的，是根据此配置，将指标拉平为：
    # {os: ios, client: NA, 指标：指标值}
    # {os: ios, client: MB, 指标：指标值}
    # {os: android, client: NA, 指标：指标值}
    # {os: android, client: MB, 指标：指标值}

    # groups中的3个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # type： file和index两种，file表示按该维度来划分文件
    # key：表示该字段入mongoDB后的查询key
    groups = [{
        "attribute": "file_group",
        "type": "file",
        "key": "client",
    }, {
        "attribute": "index_group",
        "type": "index",
        "key": "os",
    }]

    # 规定了该维度下，value的取值有哪些选项
    # name：value取值项
    # query：mongoDB中查询的key与对应的value，此配置与上级的查询配置重叠。
    index_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "ios",
        "query": {"os": "ios"},
    }, {
        "name": "android",
        "query": {"os": "android"},
    }]

    file_group = [{
        "name": "total",
        "query": {},
    }, {
        "name": "NA",
        "query": {"client": "NA"},
    }, {
        "name": "MB",
        "query": {"client": "MB"},
    }]

    # 计算完成的所有指标，最后会按照index_order中的顺序写入文件
    index_order = [
    ]

    def __init__(self, date):
        self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()
        self.user_path = []

    def _percent_statist(self, key, value_map, index_map):
        self._statist(index_map[key]["numerator"], value_map, index_map)
        self._statist(index_map[key]["denominator"], value_map, index_map)
        numerator = value_map[index_map[key]["numerator"]]
        denominator = value_map[index_map[key]["denominator"]]
        value_map[key] = float(numerator) / denominator if denominator else 0

    def _add_statist(self, key, value_map, index_map):
        if type(index_map[key]["index"]) == types.StringType:
            index_map[key]["index"] = [index_map[key]["index"]]
        add_result = 0
        for add_index in index_map[key]["index"]:
            self._statist(add_index, value_map, index_map)
            add_result += value_map[add_index]
        value_map[key] = add_result

    def _sub_statist(self, key, value_map, index_map):
        self._statist(index_map[key]["minuend"], value_map, index_map)
        self._statist(index_map[key]["subtrahend"], value_map, index_map)
        minuend = value_map[index_map[key]["minuend"]]
        subtrahend = value_map[index_map[key]["subtrahend"]]
        value_map[key] = minuend - subtrahend

    def _count_statist(self, key, value_map, index_map):
        value_map[key] = self.log_collection.find(index_map[key]["query"]).count()

    def _distinct_count_statist(self, key, value_map, index_map):
        if type(index_map[key]["distinct_key"]) == types.StringType:
            index_map[key]["distinct_key"] = [index_map[key]["distinct_key"]]
        group = {}
        group["_id"] = {field: "$" + field for field in index_map[key]["distinct_key"]}
        value = list(self.log_collection.aggregate([
            {"$match": index_map[key]["query"]},
            {"$group": group},
            {"$group": {"_id": "", "count": {"$sum": 1}}},
        ]))
        if value:
            value_map[key] = value[0]["count"]
        else:
            value_map[key] = 0

    def _sum_statist(self, key, value_map, index_map):
        value = list(self.log_collection.aggregate([
            {"$match": index_map[key]["query"]},
            {"$group": {"_id": "", "sum": {"$sum": "$" + index_map[key]["field"]}}},
        ]))
        if value:
            value_map[key] = value[0]["sum"]
        else:
            value_map[key] = 0

    def _avg_statist(self, key, value_map, index_map):
        value = list(self.log_collection.aggregate([
            {"$match": index_map[key]["query"]},
            {"$group": {"_id": "", "avg": {"$avg": "$" + index_map[key]["field"]}}},
        ]))
        if value:
            value_map[key] = value[0]["avg"]
        else:
            value_map[key] = 0

    def _map_reduce_statist(self, key, value_map, index_map):
        results = self.log_collection.map_reduce(
            Code(index_map[key]["map"]), Code(index_map[key]["reduce"]),
            "results", query=index_map[key]["query"])
        value_map[key] = index_map[key]["local"](results.find())

    def _output_statist(self, key, value_map, index_map):
        results = self.log_collection.find(index_map[key]["query"], index_map[key].get("fields"))
        value_map[key] = index_map[key]["local"](results)

    def _get_baiduid_statist(self, key, value_map, index_map):
        """
        获取百度uid函数
        :param key:
        :param value_map:
        :param index_map:
        :return:
        """
        # 根据product名字，创建文件夹，并返回路径\
        logging.info("getting baiduid list")
        path = self._get_path()

        filename = os.path.join(path, "baiduid.txt")
        fp = open(filename, "w+")
        logging.info("postion: %s" % filename)

        query = copy.deepcopy(index_map[key]["query"])
        query["source"] = "_baiduid_" + query["source"]

        pipeline = [{"$match": query},
                    {"$group": {'_id': "$baiduid", 'count': {'$sum': 1}}}]
        cursor = self.log_collection.aggregate(pipeline, allowDiskUse=True)
        for result in cursor:
            fp.write(result["_id"] + "\n")
        fp.close()

    def _user_path_statist(self, key, value_map, index_map):
        """
        分析用户路径函数，举例
        {
            "/zici/s": {
                "query": {},
                "type": "user_path",
                "no_group": True    //该字段表示，分组配置不生效
            }
        },
        :param key: 指标名
        :param value_map: 存储指标结果，此处不用
        :param index_map: 指标配置
        :return:
        """
        destination_url = index_map[key]["query"]["url"]
        query = copy.deepcopy(index_map[key]["query"])
        # user_path的source用特定，利用index提高速度
        query["source"] = "_user_path_" + query["source"]
        self.loops = 0
        self.target_source = dict()
        # 记录当前在计算属于哪一个转换页面的路径
        self.destination_url = destination_url
        print query, destination_url
        self.get_path_recursion(index_map[key]["loops"], query, [destination_url])

    def get_path_recursion(self, loops, query, target_list):
        """
        递归获取用户路径分析。
        :param loops:
        :param query:
        :param target_list:
        :return:
        """
        if self.loops >= loops or not target_list:
            return
        self.loops += 1

        local_source = set()
        for one_target in target_list:
            if one_target in self.target_source:
                continue
            self.target_source[one_target] = target_list
            source_set = self.get_one_path(query, one_target)
            local_source.update(source_set)
        # local_source -= self.source_set
        self.get_path_recursion(loops, query, local_source)

    def get_one_path(self, query, target):
        """
        获取一个目标的路径
        :param query:
        :param target:
        :return:
        """
        tmp_q = copy.deepcopy(query)
        tmp_q["url"] = target
        # obj = [
        #     {"$match": tmp_q},
        #     {"$group": {"_id": "$referr", "count": {"$sum": 1}}}
        # ]
        value = list(self.log_collection.find(tmp_q,
                                              {"_id": 0},
                                              no_cursor_timeout=True))
        source_set = set()
        other = 0
        direct = 0
        wise_search = 0
        for item in value:
            # 直接访问
            referr = item["referr"]
            count = item["value"]
            if not referr or referr == "-":
                direct += count
            # elif "from=" in referr:
            #     wise_search += count
            # 访问量小或者其他分辨不出的来源
            elif count < 100:
                other += count
            # 死循环去除。防止 A流向b，b又流向a这种图
            elif referr in self.target_source[target]:
                continue
            else:
                # 记录计算过的节点对应图。 { target:[source1, source2]}
                self.target_source[target].append(referr)
                # 防止重复计算某一节点的。记录计算过的节点
                source_set.add(referr)
                # 记录结果
                self.user_path.append({
                    "@index": self.destination_url,
                    "source": referr,
                    "target": target,
                    "value": count
                })
        if direct:
            self.user_path.append({
                "@index": self.destination_url,
                "source": u"直接访问",
                "target": target,
                "value": direct
            })
        if wise_search:
            self.user_path.append({
                "@index": self.destination_url,
                "source": u"wise大搜",
                "target": target,
                "value": other
            })
        if other:
            self.user_path.append({
                "@index": self.destination_url,
                "source": "other",
                "target": target,
                "value": other
            })
        return source_set

    def _statist(self, key, value_map, index_map):
        u"""
        统计类型总入口，具体各个类型的统计走_[type name]_statist方法
        """
        if value_map.get(key) is not None:
            return
        if index_map[key]["type"] == "":
            raise Exception(u"未指定指标类型")
        try:
            func = getattr(self, "_" + index_map[key]["type"] + "_statist")
        except AttributeError:
            raise Exception(u"未知的指标类型")
        else:
            func(key, value_map, index_map)

    def make_regex(self, query):
        u"""
        解决正则表达式不能deepcopy的问题
        配置中正则表达式写作/reg/，deepcopy后编译
        """
        if type(query) == dict:
            for key in query:
                if type(query[key]) == dict or type(query[key]) == list:
                    self.make_regex(query[key])
                elif type(query[key]) == types.StringType:
                    if query[key].startswith("/") and query[key].endswith("/"):
                        query[key] = query[key][1:-1]
                        query[key] = re.compile(query[key])
        elif type(query) == list:
            for key in xrange(len(query)):
                if type(query[key]) == dict or type(query[key]) == list:
                    self.make_regex(query[key])
                elif type(query[key]) == types.StringType:
                    if query[key].startswith("/") and query[key].endswith("/"):
                        query[key] = query[key][1:-1]
                        query[key] = re.compile(query[key])

    def statist_group(self, match, keys=None):
        """
        统计计算一个具体group的指标值
        :param match: 根据各级别group，合成的一个query的json串。
        比如group1的查询条件为{"os": "ios"}，group2的为{"client": "NA"}
        那么match为{"os": "ios", "client": "NA"}
        :param keys: 如果各级别group有限定key，则keys为这些key的合集list，且已经去重了
        :return: 指标json串，格式为{"指标1"："指标值1", "指标2"："指标值2", ......}
        """
        index_map = copy.deepcopy(self.index_map)
        default_query = copy.deepcopy(self.default_query)
        self.make_regex(index_map)
        self.make_regex(default_query)
        if keys is None:
            keys = index_map.keys()
        else:
            try:
                index_map = {key: index_map[key] for key in keys}
            except KeyError as e:
                logging.info("[ERROR]index not exists:%s" % e.message)
        # 补充分组条件
        for key in keys:
            value = index_map[key]
            if "query" in value:
                # no_group 字段表示该指标不受分组控制
                if value.get("no_group"):
                    value["query"].update(default_query)
                else:
                    value["query"].update(match)
                    value["query"].update(default_query)

        value_map = {}
        # 开始计算指标
        for key in keys:
            value = index_map[key]
            # 不受分组控制标签单独控制
            if value.get("no_group"):
                # 已经执行过就直接跳过，不再执行
                if self.index_map[key].get("has_executed"):
                    continue
                # 否则执行，并置标志位
                else:
                    self._statist(key, value_map, index_map)
                    self.index_map[key]["has_executed"] = True
            # 不受分组控制的直接执行
            else:
                self._statist(key, value_map, index_map)

        return value_map

    def _iter_groups(self, layer=0):
        """
        根据self.groups中的配置，将配置拉平
        如 OS(ios/android)->client(NA/MB)->指标
        那么此处list就写上2组。如果类型为file，表示最终会按照该维度进行文件分组
        最终目的，是根据此配置，将指标拉平为：
        [
        [{ios配置}，{NA配置}],
        [{ios配置}，{MB配置}],
        [{android配置}，{NA配置}],
        [{android配置}，{MB配置}],
        ]
        :param layer:
        :return:
        """
        total_layer = len(self.groups) - 1
        group_info = self.groups[layer]
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
                for ext in self._iter_groups(layer + 1):
                    tmp = copy.deepcopy(ret)
                    yield tmp + ext

    def statist(self):
        u"""统计入口
        输出形式：
        {
            "group01 name": {
                "group11 name": {index map...},
                "group12 name": {index map...},
            },
        }
        """
        ret = {}
        # added by xulei12@baidu.com 2016.6.20 推送到kgdc前台mongo
        ret_for_front = []
        # add ended

        # 将各级别的query字段合成一个query，并将所有的key合成一个list
        for group in self._iter_groups():
            match = {}
            keys = None
            for g in group:
                match.update(g["query"])
                if g.get("index") is not None:
                    if keys is None:
                        keys = g["index"]
                    else:
                        keys = list(set(keys) & set(g["index"]))
            # 这段但的目的是为了构造上述返回的json串结构。从a->b->指标
            # 的结构中，根据group的配置通过一层层的循环获取到“指标”这一层的json引用。
            # 然后赋值为计算结果。
            # xulei12@baidu.com   2016-06-20 为可阅读性改写逻辑写法
            tmp_ret = ret
            for item in group[0:-1]:
                if tmp_ret.get(item["name"]) is None:
                    tmp_ret[item["name"]] = {}
                tmp_ret = tmp_ret[item["name"]]
            index_json = self.statist_group(match, keys)
            tmp_ret[group[-1]["name"]] = index_json

            # added by xulei12@baidu.com 2016.6.20 推送到kgdc前台mongo
            ret_for_front.extend(self.format_index_for_mongo(index_json, group))
            # add ended
        return ret, ret_for_front

    def _get_path(self):
        date = self.date
        module_name = self.__module__.split(".")[-1]
        module_path = os.path.join(conf.OUTPUT_DIR, "midpage", module_name)
        # 清理30天前的输出
        lib.tools.clear_files(module_name, 30)
        output = os.path.join(module_path, str(date))
        if not os.path.exists(output):
            os.makedirs(output)
        return output

    def write_result(self, filename, rows):
        u"""将结果输出到文件
        """
        num = 100
        with open(filename, "w") as fp:
            for row in rows:
                row.insert(0, num)
                num += 1
                row = [unicode(r) for r in row]
                row = "\t".join(row)
                fp.write(row.encode("utf-8"))
                fp.write("\n")

    def get_rows(self, result, group, filename):
        u"""将一个分组的输出转换成数组形式，便于后续输出到文件
        """
        rows = []
        group_name = []
        index_result = result
        for g in group:
            group_name.append(g["name"])
            index_result = index_result[g["name"]]
        for index in self.index_order:
            if index in index_result:
                group_name = copy.deepcopy(group_name)
                row = group_name + [index]
                row.append(index_result[index])
                rows.append(row)
            else:
                group_sign = [g["name"] for g in group]
                group_sign = ",".join(group_sign)
                logging.info("[NOTICE]index not exist, index:%s, group:%s, file:%s" %
                             (index, group_sign, filename))
        return rows

    def save_result(self, result):
        # 根据product名字，创建文件夹，并返回路径
        path = self._get_path()
        # 是否根据分组分文件存储标志位
        file_sign = self.groups[0]["type"] == "file"
        file_index = {}
        for group in self._iter_groups():
            # 决定是否要分文件存储
            if not file_sign:
                filename = "total"
                _group = group
                _result = result
            else:
                filename = group[0]["name"]
                _group = group[1:]
                _result = result[filename]
            # 拉平数据
            rows = self.get_rows(_result, _group, filename)
            if file_index.get(filename) is None:
                file_index[filename] = rows
            else:
                file_index[filename].extend(rows)

        for filename, rows in file_index.items():
            filename = os.path.join(path, "%s.txt" % filename)
            self.write_result(filename, rows)

    def save_for_kgdc(self, result):
        # 根据product名字，创建文件夹，并返回路径
        module_name = self.__module__.split(".")[-1]
        path = os.path.join(conf.OUTPUT_DIR, "kgdc/%s" % module_name)
        if os.path.exists(path):
            # 清理30天之前的产出
            lib.tools.clear_files(path, 30)
        else:
            os.makedirs(path)
        file_name = os.path.join(path, "%s.txt" % self.date)
        with open(file_name, "w") as fp:
            for row in result:
                fp.write(json.dumps(row, ensure_ascii=False).encode("utf-8"))
                fp.write("\n")
        # 存储用户画像文件
        if self.user_path:
            file_name = os.path.join(path, "%s_user_path.txt" % self.date)
            with open(file_name, "w") as fp:
                for row in self.user_path:
                    fp.write(json.dumps(row, ensure_ascii=False).encode("utf-8"))
                    fp.write("\n")

    def format_index_for_mongo(self, index_json, group):
        """
        将每个group计算出来的指标，json格式，加上group信息，组成kgdc需要的前台对应字符串
        xulei12@baidu.com 2016.6.20
        :param index_json: 指标字符串,举例 {"pv": 1200, "uv": 3600}
        :param group: group结构定义，举例
         [
         {'query': {'client': 'NA'}, 'name': 'NA', 'key': 'client'},
        {'query': {}, 'name': 'total', 'key': 'os'}
        ]
        :return: 举例[{'@index':'pv', '@value': 1200, 'client': 'NA', 'os': 'total'},.....]
        """
        ret = []
        group_json = {}
        for group_item in group:
            group_json[group_item["key"]] = group_item["name"]

        for key, value in index_json.items():
            index_item = {
                '@index': key,
                '@value': value
            }
            index_item.update(group_json)
            ret.append(index_item)

        return ret

    def run(self):
        # modified by xulei12@baidu.com 2016.6.20
        index_result, index_kgdc = self.statist()
        self.save_result(index_result)
        self.save_for_kgdc(index_kgdc)
        # modify ended
