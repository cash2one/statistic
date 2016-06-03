# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : import_summary_data.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库
import os
import sys
import time
import logging
import traceback
# 第三方库

# 自有库
from conf import conf
import error

_self_path = os.path.dirname(os.path.abspath(__file__))
_send_email_bin = os.path.join(_self_path, "sendEmail")


def send_msg(mobile_list, msg):
    if len(mobile_list) > 0:
        for mobile in mobile_list:
            cmd = 'gsmsend -s emp02.baidu.com:15003 %s@"%s"' % (mobile, msg)
            logging.info("Run Cmd: " + cmd)
            cmd = cmd.encode("gb18030")
            os.popen(cmd)


def send_email(addr, title, text, html=False, host="system", cc=""):
    u"""
    注意：addr必须为完整的E-mail地址
    :param addr:
    :param title:
    :param text:
    :param html:
    :param host:
    :param cc:
    :return:
    """
    if "@" not in host:
        host += "@kgdc.baidu.com"

    opt = ""
    if html is True:
        opt += " -o message-content-type=html"
    if cc:
        opt += " -cc '%s'" % cc
    cmd = "%s -u '%s' -t '%s' -m '%s' -s 'hotswap-in.baidu.com' -f '%s' -o message-charset=utf-8 %s" % (_send_email_bin, title, addr, text, host, opt)
    cmd = cmd.replace("\n", "\\n")
    cmd = cmd.encode("utf-8")
    logging.info("Run Cmd: " + cmd)
    os.popen(cmd)


def wget(url, path, replace=True):
    u"""
    下载ftp,http,https
    :param url:
    :param path:
    :param replace:
    :return:
    """
    if os.path.exists(path):
        logging.info("path exists:%s" % path)
        if replace is False:
            return
        else:
            logging.info("remove...")
            os.remove(path)
    cmd = "wget -q -O %s %s" % (path, url)
    logging.info(cmd)
    code = os.system(cmd)
    if code != 0:
        logging.info("wget error:%d" % code)
        raise error.DownloadError(u"wget error")


def iter_list(all_data, num=50):
    u"""
    将一个list拆分成多组list，每组最多num个
    :param all_data:
    :param num:
    :return:
    """
    ret = all_data[:]
    while len(ret) >= num:
        yield ret[:num]
        ret = ret[num:]
    if ret:
        yield ret


def run_main_cmd(cmd, args=None):
    u"""

    :param cmd: main.py命令
    :param args: main.py参数
    :return:
    """
    base_dir = conf.BASE_DIR
    python_cmd = "python"
    if args is None:
        args = []
    args = ["'" + unicode(arg) + "'" for arg in args]
    args = " ".join(args)
    cmd_str = "cd %s;source ~/.bash_profile;nohup %s main.py %s %s 1>/dev/null 2>&1 &" %\
              (base_dir, python_cmd, cmd, args)
    logging.info(cmd_str)
    os.system(cmd_str)