#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：
该文件在hadoop执行机上跑
File   : mapred.py

Authors: xulei12@baidu.com
Date   : 2016/12/16
Comment: 
"""
# 系统库
import sys
import json
import logging
# 第三方库

# 自有库
import parse


class HadoopBase(object):
    def __init__(self, mapred, source):
        """
        初始化
        :param mapred:
        :param source:
        :return:
        """
        self.source = source
        self.mapred = mapred

    def _mapper_in(self):
        """

        :return:
        """
        return sys.stdin

    def _reducer_in(self):
        """

        :return:
        """
        return sys.stdin

    def _emit(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        print "%s\t%s" % (key, value)

    def mapper(self):
        """

        :return:
        """
        for line in self._mapper_in():
            try:
                line = line.strip()
                line = parse.analysis_line(line, self.source)
                if line:
                    self._emit(line["url"].encode("utf-8"),
                               json.dumps(line, ensure_ascii=False).encode("utf-8"))
            except Exception as e:
                logging.error(e)
                logging.error(line)
                continue

    def reducer(self):
        """

        :return:
        """
        counts = dict()
        for line in self._reducer_in():
            try:
                line = line.strip()
                kv = line.split("\t")
                value = json.loads(kv[1])
                url = value["url"]
                referr = value["referr"]
                counts.setdefault(url, dict())
                counts[url].setdefault(referr, 0)
                counts[url][referr] += 1
                self._emit(kv[0], kv[1])
            except Exception as e:
                continue

        if counts:
            for url in counts:
                for referr in counts[url]:
                    self._emit(url,
                               '{"source": "%s", "url": "%s", "referr": "%s", "value": %s}' % (
                                   "_user_path_" + self.source,
                                   url,
                                   referr,
                                   counts[url][referr]))

    def run(self):
        """

        :return:
        """
        if self.mapred == "map":
            self.mapper()
        elif self.mapred == "reduce":
            self.reducer()


class HadoopTest(HadoopBase):
    def __init__(self, source, input_file, output_file):
        """

        :param source:
        :param input_file:
        :param output_file:
        :return:
        """
        super(HadoopTest, self).__init__("map", source)
        self.collection = []
        self.mapper_out = None
        self.input_file = input_file
        self.output_fie = output_file

    def _mapper_in(self):
        """

        :return:
        """
        with open(self.input_file) as fp:
            for line in fp:
                line = line.rstrip()
                yield line

    def _reducer_in(self):
        """

        :return:
        """
        return self.mapper_out

    def _emit(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        self.collection.append("%s\t%s" % (key, value))

    def out_put(self):
        """

        :return:
        """
        fp = open(self.output_fie, "w+")
        # fp.write("\n".join(self.collection))
        for line in self.collection:
            try:
                fp.write(line)
                fp.write("\n")
            except Exception as e:
                logging.error(e)
        fp.close()

    def run(self):
        """

        :return:
        """
        print "begin to mapper"
        self.mapper()
        print "shuffle and sort"
        self.mapper_out = self.collection
        self.collection = []
        self.mapper_out.sort()
        print "begin to reduce"
        self.reducer()
        print "output data"
        self.out_put()


def main(mapred, source):
    """

    :param mapred:
    :param source:
    :return:
    """
    a = HadoopBase(mapred, source)
    a.run()


def test():
    """

    :return:
    """
    a = HadoopTest("hanyu", "/home/work/temp/web0.nj02", "/home/work/temp/test_hadoop.txt")
    a.run()


if __name__ == '__main__':
    mapred = sys.argv[1]
    source = sys.argv[2]
    main(mapred, source)
