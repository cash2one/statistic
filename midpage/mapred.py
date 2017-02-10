#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : mapred.py

Authors: xulei12@baidu.com
Date   : 2017/1/4
Comment: 
"""
# 系统库
import sys
import logging
import importlib
# 第三方库

# 自有库


def main(date, product, mapred):
    """
    远端运行入口
    :param date: 运行日期
    :param product: 产品名字，对应 products_mapred下文件名
    :param mapred: 枚举值mapper 或者 reducer
    :return:
    """
    module = importlib.import_module("products_mapred.%s" % product)
    a = module.Mapred(date, product)
    if mapred == "analysis":
        a.analysis()
    if mapred == "mapper":
        a.mapper()
    elif mapred == "reducer":
        a.reducer()
    else:
        logging.error("param error: product=%s, date=%s, mapred=%s" % (product, date, mapred))


def test(date="20170110", product="test"):
    """
    在本地测试程序远程部分
    :param date:
    :param product:
    :return:
    """
    module = importlib.import_module("midpage.products_mapred.%s" % product)
    in_file = "/home/work/temp/st01-ps-ae-app4.st01.baidu.com-frontend_access.log.2017020612.363"
    out_file = "/home/work/temp/nj02.output"
    a = module.Mapred(date, product, test_mode=True, in_file=in_file, out_file=out_file)
    a.test()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        logging.error("param is too short\n python mapred.py {date} {product} {mapper|reducer}")
        exit(1)
    date = sys.argv[1]
    product = sys.argv[2]
    mapred = sys.argv[3]
    main(date, product, mapred)
