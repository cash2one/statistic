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
"""
# 标准库
import os
from importlib import import_module
# 第三方库

# 自有库
from lib import tools
from conf import conf
import midpagedb


def get_proudcts():
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
    if products:
        if type(products) != list:
            products = [products]
    else:
        products = get_proudcts()

    for product in products:
        tools.log('[INFO]product %s statis start' % product)
        try:
            module = get_product_module(product)
        except:
            tools.log(u"get product module error:%s" % product)
            tools.ex()

        if not hasattr(module, 'source'):
            module.source = "qianxun"
        if sources and module.source not in sources:
            tools.log('[INFO]product %s source not match' % product)
            continue
        try:
            run_product_module(date, module)
        except:
            tools.log(u"run product module error:%s" % product)
            tools.ex()
        tools.log('[INFO]product %s statis end' % product)
