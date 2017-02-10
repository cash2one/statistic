#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : tiyu_entity.py

Authors: xulei12@baidu.com
Date   : 2017/2/10
Comment: 
"""
# 系统库
import copy
import json
import logging
# 第三方库

# 自有库
import tiyu
try:
    import common
except:
    import midpage.common as common


class Mapred(tiyu.Mapred):
    """
    为了计算各个实体的访问量
    """
    # 指标配置
    index_map = {
        # 每一项即为一个指标，KEY就是指标名，中文务必加 u
        u"entity": {
            # query为计算指标需要用的参数或者字段
            "query": {
            },
            # mapper 与 reducer 阶段执行的处理过程， 比如count 找 _count
            "mapper": "group_by",
            "reducer": "merge_index",
            # local是mapred任务完成后，在本地执行的操作
            "local": "write_file",
            "config": {
                "mapper": {
                    "key": "id",
                    "num": 10,
                },
                "reducer": 10,
                "local": "%s.txt"
            },
            # 该指标对应的维度信息，在下面定义
            "group_name": "entity_groups"
        }
    }
    ###################################
    groups = ["entity_groups"]
    # groups中的2个字段含义：
    # attribute: 该字段的详细配置，也就是下面紧跟着的dict名
    # key：表示在数据中，该维度对应的key名字。
    # 该配置 是对下面每个维度的一个汇总。可以存在多个维度汇总，名字自定义。
    entity_groups = [{
        "key": "page",
        "attribute": "page_group"
    }]
    ###################################
    # 具体每个维度配置
    # 规定了该维度下，value的取值有哪些选项，名称自定义，上面的维度汇总配置引用
    # name：value取值项
    # query：匹配中该维度的查询条件
    page_group = [{
        "name": "detail",
        "query": {"url": "/detail"}
    }, {
        "name": "player",
        "query": {"url": "/player"}
    }, {
        "name": "news",
        "query": {"url": "/news"}
    }, {
        "name": "videodetail",
        "query": {"url": "/videodetail"}
    }]

    def analysis(self):
        """
        改写基类中的analysis，用于特殊处理
        :return:
        """
        key_map = {
            "/detail": "name",
            "/player": "id",
            "/news": "id",
            "/videodetail": "name"
        }
        for line in self._mapper_in():
            try:
                line = line.strip()
                line = self.analysis_line(line)
                if line:
                    if line["url"] in key_map:
                        key = key_map[line["url"]]
                        if key in line["query"]:
                            value = line["query"][key]
                            line["id"] = value
                            self._emit(key.encode("utf-8"),
                                       json.dumps(line, ensure_ascii=False).encode("utf-8"))
            except Exception as e:
                logging.error(e)
                logging.error(line)
                continue

    def _group_by(self, line, index_item, config=None):
        """
        :param line: 要分析的行， 已经是json格式
        :param index_item: self.index_map 中某个指标的全部配置
        :return:
        """
        key = config["key"]
        group_key = copy.deepcopy(index_item["group_key"])
        group_key.append(key)
        value = line[key]
        for group in index_item["group"]:
            if common.json_equal(line, group["query"]):
                temp_value = copy.deepcopy(group["keys"])
                temp_value[key] = value
                temp_value["@value"] = 1
                index_item.setdefault("@value", None)
                index_item["@value"] = self.contract_index(index_item["@value"],
                                                           temp_value,
                                                           group_key)

    def _mapper(self):
        """
        方法改写。做特殊处理
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
            if type(item["@value"]) != dict:
                continue
            group_key = copy.deepcopy(item["group_key"])
            key = item["config"]["mapper"]["key"]
            num = item["config"]["mapper"]["num"]
            group_key.append(key)
            all_entities = dict()
            emit_dict = None
            for record in self.expand_index(index, item["@value"], group_key):
                all_entities.setdefault(record["page"], list())
                all_entities[record["page"]].append(record)
            for page, entities in all_entities.items():
                emit_entities = sorted(entities,
                                       key=lambda entity: entity["@value"])
                for entity in emit_entities[-num:]:
                    emit_dict = self.contract_index(emit_dict, entity, group_key)
            self.emit_lines += 1
            self._emit(index.encode("utf-8"),
                       json.dumps(emit_dict, ensure_ascii=False).encode("utf-8"))

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
            num = item["config"]["reducer"]
            group_key = copy.deepcopy(item["group_key"])
            group_key.append("id")
            all_entities = dict()
            for record in self.expand_index(index, item["@value"], group_key):
                all_entities.setdefault(record["page"], list())
                all_entities[record["page"]].append(record)
            for page, entities in all_entities.items():
                emit_entities = sorted(entities,
                                       key=lambda entity: entity["@value"])
                for record in emit_entities[-num:]:
                    self.emit_lines += 1
                    self._emit(index.encode("utf-8"),
                               json.dumps(record, ensure_ascii=False).encode("utf-8"))
    # def _reducer(self):
    #     """
    #     方法改写。做特殊处理
    #     :return:
    #     """
    #     all_entities = dict()
    #     for line in self._reducer_in():
    #         try:
    #             line = line.strip()
    #             self.valid_lines += 1
    #             kv = line.split("\t")
    #             page = kv[0].decode("utf-8")
    #             value = kv[1]
    #             value = json.loads(value)
    #             all_entities.setdefault(page, list())
    #             all_entities[page].append(value)
    #         except Exception as e:
    #             self.error_lines += 1
    #             continue
    #     for page, entities in all_entities.items():
    #         emit_entities = sorted(entities,
    #                                key=lambda entity: entity["@value"])
    #         for entity in emit_entities[-10:]:
    #             self.emit_lines += 1
    #             self._emit(entity["@index"].encode("utf-8"),
    #                        json.dumps(entity, ensure_ascii=False).encode("utf-8"))
