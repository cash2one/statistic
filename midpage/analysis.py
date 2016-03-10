# coding=utf-8
import os
import re
import time
import urlparse

from lib import tools
from conf import conf
import midpagedb
import statist

LOG_DATAS = {
    'st01-kgb-haiou1.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou1.st01/access_%s.log',
    'st01-kgb-haiou2.st01': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/st01-kgb-haiou2.st01/access_%s.log',
    'nj02-kgb-haiou1.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou1.nj02/access_%s.log',
    'nj02-kgb-haiou2.nj02': 'ftp://nj02-wd-kg14.nj02.baidu.com/home/work/seagull/online_statistics/original_log/nj02-kgb-haiou2.nj02/access_%s.log',
}


BASE_REG = re.compile(r"^([0-9\.]+) (.*) (.*) (?P<time>\[.+\]) " + \
    r"\"(?P<request>.*)\" (?P<status_code>[0-9]{3}) (\d+) " + \
    r"\"(?P<referr>.*)\" \"(?P<cookie>.*)\" \"(?P<user_agent>.*)\" " +\
    r"(?P<cost_time>[0-9\.]+) ([0-9]+) ([0-9\.]+) ([0-9\.]+) (.+) (.*) " +\
    r"\"(.*)\" (\w*) (\w*) (\d+) ([0-9\.]+)$")
BAIDUID_REG = re.compile(r"BAIDUID=(?P<id>.+?);")
IOS_REG = re.compile(r"(?i)Mac OS X")
ANDROID_REG = re.compile(r"(?i)android")
NA_REG = re.compile(r"(xiaodurobot|dueriosapp|duerandroidapp)")
MB_REG = re.compile(r"baiduboxapp")

def clear_db():
    db = midpagedb.DateLogDb()
    db.clear()


def get_data(date):
    files = []
    midpage_dir = os.path.join(conf.DATA_DIR, "midpage/%s" % date)
    if not os.path.exists(midpage_dir):
        os.makedirs(midpage_dir)
    for file_name, log_ftp in LOG_DATAS.items():
        file_name = os.path.join(midpage_dir, file_name)
        log_ftp = log_ftp % date
        tools.wget(log_ftp, file_name)
        files.append(file_name)
    return files


def iter_file(files):
    for file_name in files:
        for line in open(file_name):
            line = line.rstrip("\r\n").decode("utf-8")
            yield line


def parse_query(query):
    query = query.encode('utf-8')
    query = urlparse.parse_qs(query)
    try:
        query = {k.decode('utf-8'):v[0].decode('utf-8') for k, v in query.items() if "." not in k}
    except:
        try:
            query = {k.decode('cp936'):v[0].decode('cp936') for k, v in query.items() if "." not in k}
        except:
            tools.log("[ERROR QUERY]%s" % query)
            return {}
    return query


def analysis_line(line):
    match = BASE_REG.match(line)
    if match is None:
        tools.log("[NOT MATCH LOG]%s" % line)
        return
    ret = {}
    request = match.group("request")
    request = request.split()
    if len(request) == 3:
        request = request[1]
    else:
        tools.log("[ERROR LOG]%s" % line)
        return
    request = urlparse.urlparse(request)
    ret["url"] = request.path
    ret["query"] = parse_query(request.query)
    number_keys = ["review_num","image_num","tuangou_num"]
    for number_key in number_keys:
        if number_key in ret["query"]:
            try:
                ret["query"][number_key] = int(ret["query"][number_key])
            except:
                pass
    user_agent = match.group("user_agent")
    ret["user_agent"] = user_agent
    ret["status_code"] = match.group("status_code")
    ret["cost_time"] = match.group("cost_time")
    cookie = match.group("cookie")
    bdid = BAIDUID_REG.search(cookie)
    ret["cookie"] = cookie
    if bdid:
        ret["baiduid"] = bdid.group("id")
    else:
        ret["baiduid"] = ""
    referr = match.group("referr")
    referr = urlparse.urlparse(referr)
    ret["referr"] = referr.path
    ret["referr_query"] = parse_query(referr.query)
    if IOS_REG.search(user_agent):
        ret["os"] = "ios"
    elif ANDROID_REG.search(user_agent):
        ret["os"] = "android"
    else:
        ret["os"] = "other"
    if NA_REG.search(user_agent):
        ret["client"] = "NA"
    elif MB_REG.search(user_agent):
        ret["client"] = "MB"
    else:
        ret["client"] = "other"
    return ret


def save_log(files):
    logs = []
    db = midpagedb.DateLogDb()
    error_num = 0
    log_num = 0
    for line in iter_file(files):
        log_line = analysis_line(line)
        if log_line:
            logs.append(log_line)
            log_num += 1
        else:
            error_num += 1
        if len(logs) > 500:
            db.insert_log(logs)
            logs = []
    if logs:
        db.insert_log(logs)
    tools.log("log num:%s" % log_num)
    tools.log("error log num:%s" % error_num)


def main(date):
    midpagedb.DateLogDb.set_date(date)
    clear_db()
    files = get_data(date)
    #files = ['/home/work/kgdc-statist/kgdc-statist/data/20160111/midpage/nj02-kgb-haiou1.nj02']
    tools.log("开始解析日志....")
    save_log(files)
    #开启全量统计
    statist.main(date)
