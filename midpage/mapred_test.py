#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : mapred_test.py

Authors: xulei12@baidu.com
Date   : 2016/12/15
Comment:

Authors: xulei12@baidu.com
Date   : 2016/02/03
Comment: 已废弃
"""
# 系统库
import sys
import os
import itertools
from optparse import OptionParser

# 第三方库

# 自有库

__version__ = "0.1.0"


class MRContext(object):
    """
    ''' mapreduce job context '''
    """

    def __init__(self, args, input, output):
        """

        :param args:
        :param input:
        :param output:
        :return:
        """
        self.nreduce = 1
        self.map_input_file = input
        self.map_output_file = ""
        self.reduce_input_file = ""
        self.reduce_output_file = output
        self.args = args
        self.collector = []
        self.midoutput = None
        self.module = None

    def load_script(self):
        """

        :return:
        """
        # set environ
        os.environ['map_input_file'] = self.map_input_file
        os.environ['mapred_task_is_map'] = 'true'
        os.environ['mapred_reduce_tasks'] = '1'
        os.environ['mapred_task_partition'] = '0'
        sys.argv = self.args[:]
        script = self.args[0]
        name = script[:script.rfind('.')]
        print "load module %s" % name
        self.module = __import__(name)
        setattr(self.module, "emit", self.emit)

    def run_mapper(self):
        """

        :return:
        """
        # set environ
        os.environ['mapred_task_is_map'] = 'true'
        print "start to run mapper, read form file: %s" % self.map_input_file
        if hasattr(self.module, "mapper_setup"):
            if False == getattr(self.module, "mapper_setup")():
                raise Exception("mapper setup failed")
        if not hasattr(self.module, "mapper"):
            raise Exception("no mapper function found!")
        mapper = getattr(self.module, "mapper")
        for l in open(self.map_input_file, 'r'):
            if l[-1] == '\n':
                l = l[:-1]
            mapper('', l)
        if hasattr(self.module, "mapper_cleanup"):
            if False == getattr(self.module, "mapper_cleanup")():
                raise Exception("mapper cleanup failed!")

    def run_shuffle_sort(self):
        """

        :return:
        """
        self.midoutput = self.collector
        self.collector = []
        print "start to sort, %d kv pairs" % len(self.midoutput)
        self.midoutput.sort(key=lambda x: x[0])
        print "sort finished"

    def run_reducer(self):
        """

        :return:
        """
        # set environ
        os.environ['mapred_task_is_map'] = 'false'
        print "start to run reducer"
        if hasattr(self.module, "reducer_setup"):
            if False == getattr(self.module, "reducer_setup")():
                raise Exception("reducer setup failed")
        if not hasattr(self.module, "reducer"):
            raise Exception("no reducer function found!")
        reducer = getattr(self.module, "reducer")
        for k, vs in itertools.groupby(self.midoutput, lambda x: x[0]):
            rvs = []
            for v in vs:
                rvs.append(v[1])
            reducer(k, rvs)
        if hasattr(self.module, "reducer_cleanup"):
            if False == getattr(self.module, "reducer_cleanup")():
                raise Exception("reducer_cleanup failed!")

    def output(self):
        """

        :return:
        """
        print "start to dump output to file: %s" % self.reduce_output_file
        out = open(self.reduce_output_file, 'w')
        for kv in self.collector:
            if (not kv[0]) and kv[1]:
                out.write(kv[1])
            elif kv[0] and (not kv[1]):
                out.write(kv[0])
            elif kv[0] and kv[1]:
                out.write("%s\t%s" % kv)
            out.write('\n')

    def emit(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """
        self.collector.append((k, v))


if __name__ == "__main__":
    parser = OptionParser(usage="usage: %prog [options] <python script> args1 args2 ...")
    parser.add_option("-i", "--input", default="input", help="input file path, default ./input")
    parser.add_option("-o", "--output", default="output", help="output file path, default ./output")
    parser.add_option("-m", "--map", action="store_true", default=False,
                      help="run map only, default False")
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)
    options, args = parser.parse_args()
    mapreduce = MRContext(args, options.input, options.output)
    mapreduce.load_script()
    mapreduce.run_mapper()
    if options.map == False:
        mapreduce.run_shuffle_sort()
        mapreduce.run_reducer()
    mapreduce.output()
