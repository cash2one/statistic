#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : hadoop.py
hadoop运行基类。
主要为了实现本地测试

Authors: xulei12@baidu.com
Date   : 2017/1/17
Comment: 
"""
# 系统库
import sys
import logging
# 第三方库

# 自有库


class Hadoop(object):
    """
    hadoop运行基类
    继承该类的时候，初始化实例指定测试模式，并指定输入输出文件即可
    a = Hadoop(test=True, in_file="/home/work/input.data", out_file="/home/work/output.data")
    a.test()
    """
    def __init__(self, test_mode=False, in_file=None, out_file=None):
        """

        :param test_mode: 测试模式 标记。 True/False，默认False
        :param in_file: 测试模式时的输入文件
        :param out_file: 测试模式时的输出文件
        :return:
        """
        self.test_mode = test_mode
        self.in_fine = in_file
        self.out_file = out_file
        self.collection = []
        self.mapper_out = None
        if test_mode:
            self._mapper_in = self.__mapper_in_test
            self._reducer_in = self.__reducer_in_test
            self._emit = self.__emit_test
        else:
            self._mapper_in = self.__mapper_in
            self._reducer_in = self.__reducer_in
            self._emit = self.__emit

    def __mapper_in(self):
        """

        :return:
        """
        return sys.stdin

    def __reducer_in(self):
        """

        :return:
        """
        return sys.stdin

    def __emit(self, key, value=None):
        """

        :param key:
        :param value:
        :return:
        """
        if value:
            print "%s\t%s" % (key, value)
        else:
            print key

    def __mapper_in_test(self):
        """
        本地测试用
        :return:
        """
        with open(self.in_fine) as fp:
            for line in fp:
                line = line.rstrip()
                yield line

    def __reducer_in_test(self):
        """
        本地测试用
        :return:
        """
        return self.mapper_out

    def __emit_test(self, key, value=None):
        """
        本地测试用
        :return:
        """
        if value:
            self.collection.append("%s\t%s" % (key, value))
        else:
            self.collection.append(key)

    def _out_put(self):
        """
        本地测试时使用
        :return:
        """
        fp = open(self.out_file, "w+")
        for line in self.collection:
            try:
                fp.write(line)
                fp.write("\n")
            except Exception as e:
                logging.error(e)
        fp.close()

    def mapper(self):
        """
        需要被继承类改写，被集群调用接口。
        使用self._mapper_in()来获取输入，
        使用self._emit()来输出，
        demo如下：
        for line in self._mapper_in():
            self._emit(line)
        :return:
        """
        raise NotImplementedError

    def reducer(self):
        """
        需要被继承类改写，被集群调用接口。
        使用self._reducer_in()来获取输入，
        使用self._emit()来输出，
        demo如下：
        for line in self._reducer_in():
            self._emit(line)
        :return:
        """
        raise NotImplementedError

    def test(self):
        """
        本地测试方法
        :return:
        """
        if not self.test_mode:
            print "not test mode"
            return
        if not self.in_fine:
            print "please set parameter: in_file"
            return
        if not self.out_file:
            print "please set parameter: out_file"
            return

        print "*********job started*******"
        print "begin to mapper"
        self.mapper()
        mapper_emit_lines = len(self.collection)
        print "mapper emit lines: %s" % mapper_emit_lines
        print "shuffle and sort"
        self.mapper_out = self.collection
        self.collection = []
        self.mapper_out.sort()
        print "begin to reduce"
        self.reducer()
        reducer_emit_lines = len(self.collection)
        print "output data"
        self._out_put()
        print "all over, result is in: %s" % self.out_file
        print "reducer emit lines: %s" % reducer_emit_lines


def test():
    """
    测试代码
    :return:
    """
    class HadoopTest(Hadoop):
        def mapper(self):
            for line in self._mapper_in():
                self._emit(line)

        def reducer(self):
            for line in self._reducer_in():
                self._emit(line)
    a = HadoopTest(test_mode=True,
                   in_file="/home/work/temp/nj02.short",
                   out_file="/home/work/temp/output.data")
    a.test()
