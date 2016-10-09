#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : main.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-20
Comment:

Date   : 2016-09-30
Comment: 迁移到icode
"""
# 标准库
import sys
# 第三方库

# 自有库

if __name__ == "__main__":
    from core.management import execute_from_command_line

    execute_from_command_line(sys.argv)

