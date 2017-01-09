#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : common.py

Authors: xulei12@baidu.com
Date   : 2017/1/4
Comment: 
"""
# 系统库
import types
import logging

# 第三方库

# 自有库
logging.root.setLevel(logging.ERROR)


def is_basic_types(value):
    """

    :param value:
    :return:
    """
    if (isinstance(value, types.StringType) or
            isinstance(value, types.IntType) or
            isinstance(value, types.LongType) or
            isinstance(value, types.FloatType) or
            isinstance(value, types.BooleanType)):
        return True
    else:
        return False


def basic_equal(return_unicode_string, expect_string):
    """
    基本检查单元
    :param return_unicode_string: 被检查字符串，为unicode或者UTF-8
    :param expect_string: 期望值，必须为UTF-8
    :return: 成功返回True，失败返回False
    """
    # 返回值为unicode则转码为utf-8
    if isinstance(return_unicode_string, types.UnicodeType):
        return_unicode_string = return_unicode_string.encode("utf-8")
    # 不符则提示信息
    if is_basic_types(expect_string):
        if return_unicode_string == expect_string:
            return True
    # return_unicode_string的类型不符也走默认分支
    return False


def json_equal(return_json, expect_json, not_exist_flag=False):
    """
    rest返回json串检查函数。
    可检查简单json，list，dict
    以及互相的嵌套。不过list会逐一对应检查,并不会排序
    :param return_json: rest返回的需要检查的json
    :param expect_json: 预期正确的json
    :param not_exist_flag: 为TRUE，则严格校验expect_json中的字段不存在于return_json。
    如果expect_json为一层的json结构，则要求每一个key都不存在。
    如果expect_json为多层的json结构，如a:{b:xx}, 则校验最里面一层，即要求b不存在。
    默认为FALSE
    :return:
    完全一致 True
    有一条不一样都返回 False
    """
    # logging.basicConfig(level=logging.ERROR)
    if not expect_json:
        expect_json = {}

    if True is not_exist_flag:
        for (key, value) in expect_json.items():
            if isinstance(value, types.ListType):
                if not isinstance(return_json[key], types.ListType):
                    logging.info("the expected type (List) is not the same as return type (%s)"
                                 % type(return_json[key]))
                    return False
                # 是list
                for (item, expect_item) in zip(return_json[key], value):
                    # 全部相符则递归
                    if is_basic_types(expect_item):
                        if expect_item in return_json[key]:
                            logging.info(
                                "the expected value \"%s\" exist in the value(List) of key \"%s\"" %
                                (expect_item, key))
                            return False
                    elif not json_equal(item, expect_item, True):
                        return False
            # value为dict情况
            elif isinstance(value, types.DictType):
                # 返回值的value不为dict
                if not isinstance(return_json[key], types.DictType):
                    logging.info(
                        "the expected type (Dict) of key \"%s\" is not the same as return type (%s)" %
                        (key, type(return_json[key])))
                    return False
                # 是dict则递归
                if not json_equal(return_json[key], value, True):
                    return False
            else:
                if key in return_json:
                    logging.info("return_json has key \"%s\"" % key)
                    return False

        logging.info("check_return_json is SUCCESS")
        return True
    for (key, value) in expect_json.items():
        # key不存在与返回json中
        if key not in return_json:
            logging.info("return_json has no key \"%s\"" % key)
            return False
        # value为list的情况
        if isinstance(value, types.ListType):
            # 返回值的value不为list
            if not isinstance(return_json[key], types.ListType):
                logging.info("the expected type (List) is not the same as return type (%s)" % type(
                    return_json[key]))
                return False
            # 是list
            for (item, expect_item) in zip(return_json[key], value):
                # 如果返回值的list的一个元素与期望值的元素类型不符
                if isinstance(item, types.UnicodeType):
                    item = item.encode("utf-8")
                if type(item) != type(expect_item):
                    logging.info("the types of \"%s\" 's value is not same" % key)
                    return False
                # 全部相符则递归
                if is_basic_types(expect_item):
                    if not basic_equal(item, expect_item):
                        logging.info(
                            "the returned value \"%s\" of key \"%s\" "
                            "is not same as expected value \"%s\"" %
                            (item, key, expect_item))
                        return False
                elif not json_equal(item, expect_item):
                    return False
        # value为dict情况
        elif isinstance(value, types.DictType):
            # 返回值的value不为dict
            if not isinstance(return_json[key], types.DictType):
                logging.info(
                    "the expected type (Dict) of key \"%s\" is not the same as return type (%s)" %
                    (key, type(return_json[key])))
                return False
            # 是dict则递归
            if not json_equal(return_json[key], value):
                return False
        else:
            if not basic_equal(return_json[key], value):
                logging.info(
                    "the returned value \"%s\" of key \"%s\" is not same as expected value \"%s\"" %
                    (return_json[key], key, value))
                return False
    # 所有都通过给提示信息
    logging.info("check_return_json is SUCCESS")
    return True


def find(collection, query):
    """
    类似前端lodish的find函数
    :param collection:
    :param query:
    :return: 返回值是index
    """
    for index, item in enumerate(collection):
        if json_equal(item, query):
            return index, item
    return -1, None