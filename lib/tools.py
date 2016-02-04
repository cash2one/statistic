# coding=utf-8
import os
import sys
import time
import traceback

def log(msg):
    u"""
    打印日志到错误输出
    """
    msg = "[%s]%s\n" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), msg)
    if type(msg) == unicode:
        msg = msg.encode("utf-8")
    try:
        sys.stderr.write(msg)
        sys.stderr.flush()
    except:
        pass


def ex():
    u"""
    打印错误日志
    """
    exc_info = sys.exc_info()
    exc_type, exc_value, exc_traceback = exc_info
    msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    log(msg)


def wget(url, path, replace=True):
    u"下载ftp,http,https"
    if os.path.exists(path):
        log("path exists:%s" % path)
        if replace == False:
            return
        else:
            log("remove...")
            os.remove(path)
    cmd = "wget -q -O %s %s" % (path, url)
    log(cmd)
    code = os.system(cmd)
    if code != 0:
        log("wget error:%d" % code)
        raise 


def iter_list(all_data, num=50):
    u"""
    将一个list拆分成多组list，每组最多num个
    """
    ret = all_data[:]
    while len(ret) >= num:
        yield ret[:num]
        ret = ret[num:]
    if ret:
        yield ret