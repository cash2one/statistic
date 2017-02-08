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
import copy
import json
import logging
import importlib
from multiprocessing import Process
# 第三方库

# 自有库
import base_mapred
import common
try:
    # 由于本地和hadoop运行的目录不同。
    import conf.conf as conf
    import lib.tools as tools
    # 指明是在本地运行的部分，还是在远端运行的部分
    LOCAL_RUN = True
except:
    LOCAL_RUN = False


class BaseMapredLocal(base_mapred.BaseMapred):
    """
    从BaseMapred继承而来。分成2个class的目的只是为了本地运算代码与hadoop集群运算代码隔离
    """
    def __init__(self, date, product, **kwargs):
        super(BaseMapredLocal, self).__init__(**kwargs)
        self.date = date
        if LOCAL_RUN and conf.DEVELOPING:
            self.default_dir = "/app/ps/spider/wdmqa/kgdc/test/"
        else:
            self.default_dir = "/app/ps/spider/wdmqa/kgdc/"
        self.product = product
        self.input_dir = os.path.join(self.default_dir, self.date, self.product, "input")
        self.output_dir_1 = os.path.join(self.default_dir, self.date, self.product, "output1")
        self.output_dir_2 = os.path.join(self.default_dir, self.date, self.product, "output2")
        # 用于记录需要进行交叉计算所需要的指标
        self.among_index_list = set()

        if LOCAL_RUN:
            root_path = os.path.join(conf.DATA_DIR, "midpage_hadoop")
            tools.clear_files(root_path, 7)
            # 下载到本地文件夹路径, kgdc-statist/data/midpage_hadoop/20170109/
            self.local_data_dir = os.path.join(root_path, date)
            tools.check_dir_exist(self.local_data_dir)
            # kgdc需要的指标文件夹位置. kgdc-statist/output/kgdc_hadoop/hanyu/
            self.kgdc_file_dir = os.path.join(conf.OUTPUT_DIR, "kgdc_hadoop", self.product)
            tools.check_dir_exist(self.kgdc_file_dir)
        self.fp_dict = dict()

    def calculate_index_local(self, line):
        """
        完成mapreduce任务后，在本地执行的操作。包括合并或者生成文件等。根据具体函数处理
        对应配置位于每个指标的字段  "local"
        :param line: 文本字符串
        :return:
        """
        kv = line.split("\t")
        index = kv[0]
        value = kv[1]
        if index in self.index_map:
            func = self.get_function(self.index_map[index]["local"])
            if "local" in self.index_map[index]["config"]:
                config = self.index_map[index]["config"]["local"]
                func(value, self.index_map[index], config)
            else:
                func(value, self.index_map[index])
        if index in self.among_index_list:
            value = json.loads(value)
            index_item = self.index_map[index]
            index_item.setdefault("@value", list())
            index_item["@value"].append(value)

    def calculate_index_gather(self):
        """
        收集完所有指标信息，计算一些后验指标。比如指标之间的运算，用户路径，用户画像分析
        :return:
        """
        for index, index_item in self.index_map.items():
            func_name = index_item.get("gather")
            if func_name:
                func = self.get_function(func_name)
                if "gather" in index_item["config"]:
                    config = index_item["config"]["gather"]
                    func(index, index_item, config)
                else:
                    func(index, index_item)

    def _among_indexes(self, index, index_item, config=None):
        """
        需要后验计算的指标。一个典型的配置如下
        暂时只实现了除法，其他方法下面加一两行代码即可

        "gather": "among_indexes",
        "config": {
            "gather": {
                # 后验计算需要的指标
                "index": ["rank0", "pv"],
                # 计算方法
                "method": "divide",
                # 通过第一个指标查找第二个指标时候，需要剔除的维度
                "reject_dimension": ["type"],
                # 最终需要输出的位置
                "file": "%s.txt"
            }
        },
        :param index:
        :param index_item:
        :param config:
        :return:
        """
        first_index = config["index"][0]
        second_index = config["index"][1]
        method = config["method"]
        for first_value in self.index_map[first_index]["@value"]:
            query = copy.deepcopy(first_value)
            del query["@index"]
            del query["@value"]
            if "reject_dimension" in config:
                for dimension in config["reject_dimension"]:
                    del query[dimension]
            second_value = common.find(self.index_map[second_index]["@value"], query)
            if second_value:
                second_value = second_value[0]
                if method == "divide":
                    if second_value["@value"]:
                        first_value["@value"] /= second_value["@value"]*1.0
                # todo:其他方法待补充
                else:
                    pass
                first_value["@index"] = index
                line = json.dumps(first_value, ensure_ascii=False).encode("utf-8")
                self._write_file(line, index_item, config["file"])

    def _write_file(self, line, index_item, config="%s.txt"):
        """
        生成kgdc需要的指标文件格式
        :param line:
        :param index_item:
        :param config: 默认为 "%s.txt"，文件名
        :return:
        """
        if not config:
            return
        if config not in self.fp_dict:
            file_name = os.path.join(self.kgdc_file_dir, config % self.date)
            logging.info("create file: %s" % file_name)
            self.fp_dict[config] = open(file_name, "w+")
        self.fp_dict[config].write(line + "\n")

    def _user_path_local(self, line, index_item, config=None):
        """
        用户路径处理
        :param line:
        :param index_item:
        :param config:
        :return:
        """
        line = json.loads(line)
        index_item.setdefault("user_path", list())
        index_item["user_path"].append(line)

    def _user_path_gather(self, index, index_item, config=None):
        """

        :param index:
        :param index_item:
        :param config:
        :return:
        """
        logging.root.setLevel(logging.ERROR)
        for target in config["target"]:
            target_source = dict()
            self.get_path_recursion([target], target, index_item, target_source)
        logging.root.setLevel(logging.INFO)

        file_name = os.path.join(self.kgdc_file_dir, config["file"] % self.date)
        logging.info("create file: %s" % file_name)
        fp = open(file_name, "w+")
        for line in index_item["@value"]:
            line = json.dumps(line, ensure_ascii=False).encode("utf-8")
            fp.write(line + "\n")
        fp.close()

    def get_path_recursion(self, target_list, destination, index_item, target_source):
        """

        递归获取用户路径分析。
        :param target_list:
        :param destination:
        :param index_item:
        :param target_source: index_item["user_path"]中存储所有路径的list
        :return:
        """
        # if self.loops >= loops or not target_list:
        #     return
        # self.loops += 1
        if not target_list:
            return

        local_source = set()
        for one_target in target_list:
            if one_target in target_source:
                continue
            target_source[one_target] = target_list
            source_set = self.get_one_path(destination, one_target, index_item, target_source)
            local_source.update(source_set)
        self.get_path_recursion(local_source, destination, index_item, target_source)

    def get_one_path(self, destination, target, index_item, target_source):
        """
        获取一个目标的路径
        :param destination: 最终转换页面
        :param target:
        :param index_item: index_item["user_path"]中存储所有路径的list
        :param target_source: { target:[source1, source2]}
        :return:
        """
        tmp_q = {"url": target}
        source_set = set()
        other = 0
        items = common.find(index_item["user_path"], tmp_q, all=True)
        index_item.setdefault("@value", list())
        for item in items:
            # 直接访问
            source = item["referr"]
            count = item["@value"]
            # 死循环去除。防止 A流向b，b又流向a这种图
            if source in target_source[target]:
                continue
            # 访问量小或者其他分辨不出的来源
            elif count < 100:
                other += count
            else:
                # 记录计算过的节点对应图。 { target:[source1, source2]}
                target_source[target].append(source)
                # 防止重复计算某一节点的。记录计算过的节点
                source_set.add(source)
                index_item["@value"].append(
                    {
                        "@index": destination,
                        "source": source,
                        "target": target,
                        "value": count
                    })
        return source_set

    def set_up(self):
        """
        环境准备，包括拷贝数据到本集群
        :return:
        """
        ret = 0
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
            ret = os.system(cmd)
        else:
            self.input_dir = self.SOURCE
        self.input_dir = os.path.join(self.input_dir, "*")
        return ret

    def mapred_first(self):
        """
        mapred第一个任务， 主要逻辑是解析日志，并按照baiduid分发
        :return:
        """
        current_dir = os.path.split(os.path.abspath(__file__))[0]
        cmd = "cd %s; sh ./mapred_local_first.sh %s %s %s %s %s" % (
            current_dir,
            conf.HADOOP_BIN,
            self.product,
            self.date,
            self.input_dir,
            self.output_dir_1)
        logging.info(cmd)
        return os.system(cmd)

    def mapred_second(self):
        """
        启动计算指标hadoop任务,第二个任务。主要计算过程
        :return:
        """
        current_dir = os.path.split(os.path.abspath(__file__))[0]
        cmd = "cd %s; sh ./mapred_local_second.sh %s %s %s %s %s" % (
            current_dir,
            conf.HADOOP_BIN,
            self.product,
            self.date,
            self.output_dir_1,
            self.output_dir_2)
        logging.info(cmd)
        return os.system(cmd)

    def close_files(self):
        """
        关闭文件指针
        :return:
        """
        for file_name in self.fp_dict:
            self.fp_dict[file_name].close()

    def clear_down(self):
        """
        生成指标文件，环境清理过程
        :return:
        """
        data_file_name = os.path.join(self.local_data_dir, self.product + ".data")
        if os.path.exists(data_file_name):
            os.remove(data_file_name)
        cmd = "%s fs -getmerge %s %s" % (
            conf.HADOOP_BIN,
            self.output_dir_2,
            data_file_name)
        logging.info(cmd)
        ret = os.system(cmd)
        if ret:
            logging.error(ret)
            return ret
        # 下面这行是测试代码
        # data_file_name = "/home/work/kgdc-statist/kgdc-statist/data/midpage_hadoop/20170206/tiyu.data"
        all_lines = 0
        valid_lines = 0
        error_lines = 0
        # 先标记出，需要交叉计算的指标
        for index in self.index_map:
            if "gather" in self.index_map[index]:
                if self.index_map[index]["gather"] == "among_indexes":
                    among_index_list = set(self.index_map[index]["config"]["gather"]["index"])
                    self.among_index_list.update(among_index_list)
        # 记录维度的顺序。方便后面统计
        self.get_group_key()
        # 处理每一行
        with open(data_file_name) as fp:
            for line in fp:
                all_lines += 1
                try:
                    line = line.strip()
                    self.calculate_index_local(line)
                    valid_lines += 1
                except:
                    logging.exception("error")
                    logging.error(line)
                    error_lines += 1
        logging.error("all_lines: %s" % all_lines)
        logging.error("valid_lines: %s" % valid_lines)
        logging.error("error_lines: %s" % error_lines)

        self.calculate_index_gather()

        self.close_files()

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
        ret = self.mapred_first()
        if ret:
            logging.error("error occured in mappred_first(): %s" % ret)
            return
        ret = self.mapred_second()
        if ret:
            logging.error("error occured in mapred_second(): %s" % ret)
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
    date = "20170206"
    product = "tiyu"
    module = importlib.import_module("midpage.products_mapred.%s" % product)
    a = module.Mapred(date, product, test_mode=True, in_file="", out_file="")
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


if __name__ == '__main__':
    test()
