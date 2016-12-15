#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : user_path.py

Authors: xulei12@baidu.com
Date   : 2016/12/14
Comment:
hadoop 处理代码。
计算用户路径数据。
"""
# 系统库
import sys
import json
import logging
# 第三方库

# 自有库

SOURCE = sys.argv[1]


def mapper_setup():
    return True


def mapper(k, v):
    v = v.strip()
    try:
        line = json.loads(v)
        out = {
            "source": "user_path_" + line["source"],
            "url": line["url"] if line["url"] else "-",
            "referr": line["referr"] if line["referr"] else "-"
        }
        emit(line["source"] + out["url"], json.dumps(out, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        logging.error(v)


def mapper_cleanup():
    pass


def reducer_setup():
    return True


def reducer(k, vs):
    ret = []
    counts = dict()
    logging.error(k)
    for v in vs:
        v = v.strip()
        line = json.loads(v)
        source = line["source"]
        url = line["url"]
        referr = line["referr"]
        counts.setdefault(source, dict())
        counts[source].setdefault(url, dict())
        counts[source][url].setdefault(referr, 0)
        counts[source][url][referr] += 1

    for source in counts:
        for url in counts[source]:
            for referr in counts[source][url]:
                emit(k, '{"source": "%s", "url": "%s", "target": "%s", "value": %s}' % (
                    source,
                    url,
                    referr,
                    counts[source][url][referr]))


def reducer_cleanup():
    pass
