# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : base.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-30
Comment:
"""
# 标准库
import os
import sys
import inspect
# 第三方库

# 自有库
from core import log
from conf import conf


class BaseCommand(object):
    """
    """
    log_name = None

    def __init__(self):
        pass

    def set_log(self):
        if self.log_name:
            log_name = os.path.join(conf.LOG_DIR, self.log_name)
        else:
            cmd = self.__class__.__module__
            log_name = os.path.join(conf.LOG_DIR, cmd)
        log.init_log(log_name)

    def print_help(self, prog_name, subcommand):
        u"""
        Print the help message for this command
        handle's args and __doc__
        """
        args = inspect.getargspec(self.handle).args[1:]
        usage = [
            u"Usage: %s %s %s" % (prog_name, subcommand, " ".join(args)),
            self.handle.__doc__,
        ]
        usage = "\n".join(usage)
        sys.stdout.write((usage + '\n').encode("utf-8"))

    def assert_argv(self, *args):
        pass

    def run_from_argv(self, argv):
        self.set_log()
        self.assert_argv(*argv)
        args = argv[2:]
        self.handle(*args)

    def handle(self, *args):
        """
        """
        raise NotImplementedError()

