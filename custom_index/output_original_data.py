# coding=utf-8
import os
import json
import copy
import datetime
import importlib

from conf import conf
from lib import tools
import task_db
import data_db


def get_rows(task_id, date):
    original_data = data_db.OriginalData()
    rows = original_data.find({"@create": date, "@task": int(task_id)})
    rows = list(rows)
    if len(rows) == 0:
        tools.log("[ERROR]original data not found!")
        exit(-1)
    return rows


def get_filename(task_id, date, key=None):
    floder = os.path.join(conf.OUTPUT_DIR, "custom_index")
    if not os.path.exists(floder):
        os.makedirs(floder)
    if key:
        filename = os.path.join(floder, "%s.%s.index.%s" % (task_id, key, date))
    else:
        filename = os.path.join(floder, "%s.index.%s" % (task_id, date))
    return filename


def is_index(row, condition):
    for key, value in condition.items():
        if row.get(key) != value:
            return False
    return True


def filter_index(rows, condition):
    for row in rows:
        if is_index(row, condition):
            return row["@value"]
    return None


def create_result(rows, task_id):
    config_path = os.path.join(conf.BASE_DIR, "custom_index/output_config/task%s.py" % task_id)
    if not os.path.exists(config_path):
        print "task config not exists"
        exit(-1)
    task_conf = importlib.import_module("custom_index.output_config.task%s" % task_id)
    total_line = 1

    result_config = []
    for col in task_conf.colList:
        result_config_tmp = []
        if len(result_config) == 0:
            for index in task_conf.indexMap[col]:
                index["col"] = col
                result_config_tmp.append([index])
        else:
            for c in result_config:
                for index in task_conf.indexMap[col]:
                    index["col"] = col
                    c2 = copy.deepcopy(c)
                    c2.append(index)
                    result_config_tmp.append(c2)
        result_config = result_config_tmp

    result = {}
    for config in result_config:
        condition = {c["col"]:c["key"] for c in config}
        line = [c["name"] for c in config if c.get("filename") is None]
        filename = config[0].get("filename", "")
        if filename not in result:
            result[filename] = []
        value = filter_index(rows, condition)
        if value is None:
            continue
        line.append(value)
        result[filename].append(line)

    for key, lines in result.items():
        num = 100
        for line in lines:
            line.insert(0, num)
            num += 1
    return result


def create_output(task_id, date, result):
    for key, lines in result.items():
        filename = get_filename(task_id, date, key)
        with open(filename, 'w') as fp:
            for line in lines:
                line = [unicode(col) for col in line]
                line = "\t".join(line)
                line += "\n"
                line = line.encode("utf-8")
                fp.write(line)


def main(task_id, date):
    rows = get_rows(task_id, date)
    result = create_result(rows, task_id)
    create_output(task_id, date, result)