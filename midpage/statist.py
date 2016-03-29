# coding=utf-8
import os
from importlib import import_module

from lib import tools
from conf import conf
import midpagedb


def get_proudcts():
    products_dir = os.path.join(conf.BASE_DIR, "midpage/products")
    products = os.listdir(products_dir)
    products = [product.split(".")[0] for product in products if not product.startswith("_")]
    return products


def get_product_module(product):
    module = import_module("midpage.products.%s" % product)
    return module


def run_product_module(date, module):
    product = module.Product(date)
    product.run()


def main(date, products=None):
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
        try:
            run_product_module(date, module)
        except:
            tools.log(u"run product module error:%s" % product)
            tools.ex()
        tools.log('[INFO]product %s statis end' % product)
