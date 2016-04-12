# coding=utf-8
import os
import sys
import time
import traceback

import tools
from conf import conf
import error

_self_path = os.path.dirname(os.path.abspath(__file__))
_send_email_bin = os.path.join(_self_path, "sendEmail")

def send_msg(mobile_list, msg):
    if len(mobile_list) > 0:
        for mobile in mobile_list:
            cmd = 'gsmsend -s emp02.baidu.com:15003 %s@"%s"' % (mobile, msg)
            klib.log("Run Cmd: " + cmd)
            cmd = cmd.encode("gb18030")
            os.popen(cmd)


def send_email(addr, title, text, html=False, host="system", cc=""):
    u"注意：addr必须为完整的E-mail地址"
    if "@" not in host:
        host += "@kgdc.baidu.com"

    opt = ""
    if html == True:
        opt += " -o message-content-type=html"
    if cc:
        opt += " -cc '%s'" % cc
    cmd = "%s -u '%s' -t '%s' -m '%s' -s 'hotswap-in.baidu.com' -f '%s' -o message-charset=utf-8 %s" % (_send_email_bin, title, addr, text, host, opt)
    cmd = cmd.replace("\n", "\\n")
    cmd = cmd.encode("utf-8")
    tools.log("Run Cmd: " + cmd)
    os.popen(cmd)


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
        raise error.DownloadError(u"wget error")


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


def run_main_cmd(cmd, args=None, log_path=None):
    u"""
    cmd: main.py命令
    args: main.py参数
    log: 日志地址
    """
    base_dir = conf.BASE_DIR
    python_cmd = "python"
    if log_path:
        log_str = ">> %s 2>&1" % log_path
    else:
        log_str = ""
    if args is None:
        args = []
    args = ["'" + unicode(arg) + "'" for arg in args]
    args = " ".join(args)
    cmd_str = "cd %s;source ~/.bash_profile;nohup %s main.py %s %s %s &" % \
            (base_dir, python_cmd, cmd, args, log_str)
    log(cmd_str)
    os.system(cmd_str)