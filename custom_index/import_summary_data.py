# coding=utf-8
import os
import json
import datetime

from conf import conf
from lib import tools
from lib import error
import data_db


def get_insert_date(task_id, date):
    u"""从原始表中查找需要导入summary表的数据日期
    """
    original_data = data_db.OriginalData()
    insert_date = original_data.find({"@task":task_id, "@create":{"$lte": date}}, {"@create":1})
    insert_date.sort([("@create", -1)])
    insert_date = insert_date[:1]
    insert_date = list(insert_date)
    if len(insert_date) > 0:
        insert_date = insert_date[0]["@create"]
    else:
        insert_date = None
    return insert_date


def copy_to_summary(task_id, from_date, to_date):
    daily_summary_data = data_db.DailySummaryData()
    daily_summary_data.remove({"@task":task_id, "@date": to_date})

    original_data = data_db.OriginalData()
    lines = original_data.find({"@task":task_id, "@create": from_date})
    for line in lines:
        del line["_id"]
        line["@date"] = to_date
        daily_summary_data.insert(line)


def main(task_id, date):
    tools.log("[BEGIN]task id:%s, date:%s" % (task_id, date))
    task_id = int(task_id)
    from_date = get_insert_date(task_id, date)
    if from_date is None:
        tools.log("[ERROR]in summary task, original data not find")
        raise error.Error("original data not find")
    else:
        tools.log("[INFO]from date:%s" % from_date)
        copy_to_summary(task_id, from_date, date)
    tools.log("[END]task id:%s, date:%s" % (task_id, date))