#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : statist.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-20
Comment:

根据mongo里解析的nginx日志数据统计分析各个装逼
"""
# 标准库
import os
import logging
from importlib import import_module
# 第三方库

# 自有库
from conf import conf
import midpagedb


def get_products():
    products_dir = os.path.join(conf.BASE_DIR, "midpage/products")
    products = os.listdir(products_dir)
    products = [product for product in products if product.endswith('.py')]
    products = [product.split(".")[0] for product in products if not product.startswith("_")]
    return products


def get_product_module(product):
    module = import_module("midpage.products.%s" % product)
    return module


def run_product_module(date, module):
    product = module.Product(date)
    product.run()


def main(date, products=None, sources=None):
    midpagedb.DateLogDb.set_date(date)
    # 根据命令决定是跑一个产品
    if products:
        if type(products) != list:
            products = [products]
    # 或者跑所有midpage/products 下所有非'_'开头的产品
    else:
        products = get_products()

    for product in products:
        logging.info('[INFO]product %s statis start' % product)
        try:
            module = get_product_module(product)
        except:
            logging.exception(u"get product module error:%s" % product)

        if not hasattr(module, 'source'):
            module.source = "qianxun"
        if sources and module.source not in sources:
            logging.info('[INFO]product %s source not match' % product)
            continue
        try:
            run_product_module(date, module)
        except:
            logging.exception(u"run product module error:%s" % product)
        logging.info('[INFO]product %s statis end' % product)
