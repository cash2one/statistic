#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：
运行于本地。与base_mapred隔离。base_mapred运行于mapreduce集群
File   : base_mapred_local.py

Authors: xulei12@baidu.com
Date   : 2017/1/4
Comment: 
"""
# 系统库
import os
import logging
import importlib
from multiprocessing import Process
# 第三方库

# 自有库
import base_mapred
try:
    # 由于本地和hadoop运行的目录不同。
    import conf.conf as conf
    # 指明是在本地运行的部分，还是在远端运行的部分
    LOCAL_RUN = True
except:
    LOCAL_RUN = False


class BaseMapredLocal(base_mapred.BaseMapred):
    """
    从BaseMapred继承而来。分成2个class的目的只是为了本地运算代码与hadoop集群运算代码隔离
    """
    def __init__(self, date, product):
        super(BaseMapredLocal, self).__init__()
        self.date = date
        if LOCAL_RUN and conf.DEVELOPING:
            self.default_dir = "/app/ps/spider/wdmqa/kgdc/test/"
        else:
            self.default_dir = "/app/ps/spider/wdmqa/kgdc/"
        self.product = product
        self.input_dir = os.path.join(self.default_dir, self.date, self.product, "input")
        self.output_dir = os.path.join(self.default_dir, self.date, self.product, "output")

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
            return os.system(cmd)

    def mapred(self):
        """
        启动计算指标hadoop任务
        :return:
        """
        current_dir = os.path.split(os.path.abspath(__file__))[0]
        cmd = "cd %s; sh ./mapred_local.sh %s %s %s %s %s" % (
            current_dir,
            conf.HADOOP_BIN,
            self.product,
            self.date,
            self.input_dir,
            self.output_dir)
        logging.info(cmd)
        return os.system(cmd)

    def clear_down(self):
        """
        生成指标文件，环境清理过程
        :return:
        """
        return 0

    def run(self):
        """
        主入口
        :return:
        """
        ret = self.set_up()
        if ret:
            logging.error("error occured in set_up(): %s" % ret)
            return
        ret = self.mapred()
        if ret:
            logging.error("error occured in mapred(): %s" % ret)
            return
        ret = self.clear_down()
        if ret:
            logging.error("error occured in clear_down(): %s" % ret)
            return


def run_one_product(date, product):
    """

    :param date:
    :param product:
    :return:
    """
    module = importlib.import_module("midpage.products_mapred.%s" % product)
    a = module.Mapred(date, product)
    a.run()


def get_products():
    """

    :return:
    """
    products_dir = os.path.join(conf.BASE_DIR, "midpage/products_mapred")
    products = os.listdir(products_dir)
    products = [product for product in products if product.endswith('.py')]
    products = [product.split(".")[0] for product in products if not product.startswith("_")]
    return products


def main(date, products=None):
    """

    :param date:
    :param products:
    :return:
    """
    if products:
        products = products.split(',')
    # 或者跑所有midpage/products 下所有非'_'开头的产品
    else:
        products = get_products()

    logging.info("开始解析日志....")
    # 跑hadoop任务，统计用户路径数据
    plist = []
    for product in products:
        p = Process(target=run_one_product, args=(date, product))
        plist.append(p)
        p.start()

    for p in plist:
        p.join()


def test():
    """
    测试程序本地部分
    :param product:
    :return:
    """
    date = "20170103"
    a = BaseMapredLocal(date)
    a.run()


def test_filter():
    """
    测试过滤器
    :return:
    """
    line = '117.136.81.248 - - [03/Jan/2017:12:59:59 +0800] ' \
           '"GET /static/asset/dep/asset/img/play-icon-m.png HTTP/1.0" 200 4148 ' \
           '"http://tiyu.baidu.com/detail?name=TkJBIzIwMTctMDEtMDMj5o6Y6YeRdnPli4flo6s%3D" ' \
           '"Hm_lpvt_dfcd11bffa2fa6ca44ab9ae5fe0b4a7f=1483419596; ' \
           'Hm_lvt_dfcd11bffa2fa6ca44ab9ae5fe0b4a7f=1483419201,1483419429,1483419493,1483419596;'\
           'BAIDUID=541937CDCDCC8D82C285CBA87473BB6A:FG=1; ' \
           'BIDUPSID=779B3663B5E318A39F0924166778C64F; H_WISE_SIDS=109815_102570_100614_100042_' \
           '114039_102432_110567_100100_113960_107799_113932_107316_111551_114207_112106_114000_' \
           '111462_113568_111928_111366_114197_113566_112135_110031_114132_114095_110086; PSINO=1" ' \
           '"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 ' \
           '(KHTML, like Gecko) Mobile/11D257 baiduboxapp/0_0.6.0.6_enohpi_069_046/' \
           '2.1.7_1C2%254enohPi/1099a/2C920D743047AF5F06E6A5AF65467F563B65083BDFROACMILLB/1" ' \
           'rt=0.003 11398553596525209892 10.46.142.64 10.44.9.43 tiyu.baidu.com ' \
           '"117.136.81.248" ps appEngine - 1483419599.844'
    date = "20170103"
    a = BaseMapredLocal(date)
    match = a.FILTER.match(line)
    if match:
        print match.groupdict()
    else:
        print "not match"
