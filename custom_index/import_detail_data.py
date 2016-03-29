# coding=utf-8
import os
import json
import datetime

from conf import conf
from lib import tools
import task_db
import data_db

def get_index(task_id, date, ftp):
    floder = os.path.join(conf.DATA_DIR, "custom_index/%s" % date)
    if not os.path.exists(floder):
        os.makedirs(floder)
    path = os.path.join(floder, "%s.dat" % task_id)
    if "%s" in ftp:
        ftp = ftp % date
    tools.wget(ftp, path)
    return path


MUST_KEYS = ["@topic"]
def check_line(json_line, topic):
    for key in MUST_KEYS:
        if key not in json_line:
            tools.log("[ERROR]miss key:%s=>%s" % (key, json_line))
            return False
    if json_line["@topic"] != topic:
        tools.log("[ERROR]@topic:%s=>%s" % (topic, json_line))
        return False
    return True


def save_index(path, task, date):
    topic = task.topic
    system_key = {"@task": task.id, "@create": date}
    detail_data = data_db.DetailData(date)
    detail_data.remove(system_key)
    fp = open(path)
    for line in fp:
        line = line.rstrip("\r\n").decode("utf-8")
        try:
            json_line = json.loads(line)
        except:
            tools.log("[ERROR]json error:%s" % line)
        if check_line(json_line, topic):
            json_line.update(system_key)
            detail_data.insert(json_line)


def main(task_id, date, replace_ftp=None):
    tools.log("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    task = task_db.CustomIndexTask(task_id)
    if task.task_type != "detail":
        tools.log('[ERROR]task type is %s' % task.task_type)
        exit(-1)
    #获取数据
    if replace_ftp:
        ftp = replace_ftp
    else:
        ftp = task.path
    path = get_index(task_id, date, ftp)
    #解析入库
    save_index(path, task, date)
    tools.log("[END]task id:%s, date:%s" % (task_id, date))