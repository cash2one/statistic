#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：
指标报警处理。
从redis队列中获取报警信息，从mongodb中获取历史信息
File   : alarm_process.py

Authors: xulei12@baidu.com
Date   : 2016/9/23
Comment: 
"""
# 系统库
import json
import time
import logging
# 第三方库

# 自有库
import lib.mysql_db
import core.kgdc_redis
import custom_index.data_db
import sender


def main():
    u"""
    指标报警处理主入口。
    常驻进程，死循环。
    :return:
    """
    rs = core.kgdc_redis.KgdcRedis()

    # 死循环从redis队列获取报警处理消息。
    # 无尽等待，无消息为阻塞态
    while 1:
        alarm_item = rs.pop_alarm()
        if not alarm_item:
            continue
        try:
            alarm_set = json.loads(alarm_item)
            alarm_check(alarm_set)
        except:
            logging.error("the type of alarm_set is %s" % type(alarm_item))
            logging.error(alarm_item)
            logging.exception(u"处理失败")


def alarm_check(alarm_set):
    u"""
    报警处理主流程
    :param alarm_set: redis中读取到的报警处理信息。json格式
    :return:
    """
    # 必备key检查列表
    check_list = [
        "alarm",
        "alert",
        "@value",
        "@create"
    ]
    for key in check_list:
        if key not in alarm_set:
            logging.warning(key+" is not in alarm_set")
            return False

    # 避免反复写多级key，带来可能的笔误。最多写一级字符串key
    alarm = alarm_set["alarm"]

    # alarm中必须key及合法性检查
    if "logic" not in alarm:
        logging.warning("logic is not in alarm")
        return False
    if "condition" not in alarm:
        logging.warning("condition is not in alarm")
        return False
    conditions = alarm["condition"]
    if type(conditions) != list:
        logging.warning("condition is not a list")
        return False
    if len(conditions) == 0:
        logging.warning("condition's len is 0")
        return False

    # 是否报警的标记
    alarm_flag = False
    # and逻辑，只要有一个不满足报警条件，就不报警
    if alarm["logic"] == "and":
        logging.info("logic: and")
        # 这里有个坑。如果alarm_flag放在循环后会出错。
        alarm_flag = True
        for condition in conditions:
            if not process_one_condition(condition, alarm_set):
                alarm_flag = False
                break
    # or逻辑，只要有一个满足条件，就报警。不再判断
    elif alarm["logic"] == "or":
        logging.info("logic: or")
        for condition in conditions:
            if process_one_condition(condition, alarm_set):
                alarm_flag = True
                break
    else:
        logging.warning("logic is illegal: %s" % alarm["logic"])
        return False

    # 需要告警就通告用户
    if alarm_flag:
        logging.warning("need alert user")
        alert_user(alarm_set)
        return True
    else:
        logging.info("no need to alert")
        return False


def process_one_condition(condition, alarm_set):
    u"""
    处理一条阈值检查条件
    :param condition: 检查条件的json
    :param alarm_set: redis中的全部配置
    :return:
    """
    # 必备字段检查
    if "type" not in condition:
        logging.warning("type is not in condition")
        return False

    # 绝对值检查流程。不用查mongo
    if condition["type"] == "absolute":
        logging.info("absolute judge")
        ret = judge_absolute(condition, alarm_set["@value"])
        # 需要报警，记录报警信息
        if ret:
            update_json = {
                "flag": True,
                "condition": condition,
                "@value": alarm_set["@value"],
                "@create": alarm_set["@create"],
                "@subProject": alarm_set["@subProject"],
            }
            alarm_set["alert"].update(update_json)
        return ret
    # 相对值检查流程。需要查mongo
    elif condition["type"] == "relative":
        logging.info("relative judge")
        return judge_relative(condition, alarm_set)
    else:
        return False


def judge_absolute(condition, value):
    u"""
    绝对条件判断。
    :param condition: 条件，json串
    {
        "type": "absolute",
        "percent": true,    # bool值，比较百分比还是差值。
        "operator": "<",    # 判断规则。>: 大于， <： 小于，相对于字段value
        "value": -0.5,      # 浮点数，判断阈值。
        "time": {           # 与什么时候的指标值对比，type为relative时生效。
            "unit": "day",  # 单位。week：星期；day：天； hour：小时；minute：分钟
            "num": 1,       # 与几个<单位>前的指标进行对比计算。如果该时间无指标，则会往前继续查询到最近一次的数据。
        }
    }
    :param value: 当前值
    :return:
    """
    if condition["operator"] == "<":
        logging.info("< judge")
        if value < condition["value"]:
            logging.info("@value(%s) < condition.value](%s)" % (value, condition["value"]))
            condition["alarm"] = True
            return True
        else:
            logging.info("@value(%s) is not < condition.value(%s)" % (value, condition["value"]))
            condition["alarm"] = False
            return False
    elif condition["operator"] == ">":
        logging.info("> judge")
        if value > condition["value"]:
            logging.info("@value(%s) > condition.value(%s)" % (value, condition["value"]))
            condition["alarm"] = True
            return True
        else:
            logging.info("@value(%s) is not > condition.value(%s)" % (value, condition["value"]))
            condition["alarm"] = False
            return False
    else:
        logging.warning("operator illegal: %s" % condition["operator"])
        condition["alarm"] = False
        return False


def judge_relative(condition, alarm_set):
    u"""

    :param condition: 条件，json串
    {
        "type": "absolute",
        "percent": true,    # bool值，比较百分比还是差值。
        "operator": "<",    # 判断规则。>: 大于， <： 小于，相对于字段value
        "value": -0.5,      # 浮点数，判断阈值。
        "time": {           # 与什么时候的指标值对比，type为relative时生效。
            "unit": "day",  # 单位。week：星期；day：天； hour：小时；minute：分钟
            "num": 1,       # 与几个<单位>前的指标进行对比计算。如果该时间无指标，则会往前继续查询到最近一次的数据。
        }
    }
    :param alarm_set: 全部配置json串
    :return:
    """
    # 必须key判断
    if "time" not in condition or "percent" not in condition:
        logging.warning("time or percent is not in condition")
        return False
    if "unit" not in condition["time"] or "num" not in condition["time"]:
        logging.warning("unit or num is not in condition[\"time\"")
        return False

    # string转换为tp结构
    create_tp = time.strptime(alarm_set["@create"], "%Y-%m-%d %H:%M")
    # tp结构转换为ts秒
    create_ts = time.mktime(create_tp)

    # 处理不同单位
    ts = condition["time"]["num"]
    unit = condition["time"]["unit"]
    ts_map = {
        "minute": 60,
        "hour": 60*60,
        "day": 60*60*24,
        "week": 60*60*24*7,
    }
    name_map = {
        "minute": u"分钟",
        "hour": u"小时",
        "day": u"天",
        "week": u"星期",
    }
    if unit in ts_map:
        ts *= ts_map[unit]
        condition["time"]["unit_name"] = name_map[unit]
    else:
        return False

    # 上的逆向过程转换为字符串
    last_ts = create_ts - ts
    last_tp = time.localtime(last_ts)
    last_str = time.strftime("%Y-%m-%d %H:%M", last_tp)

    # monitor中字段唯一确定一个指标监控。一个指标与不同的自定义筛选指标
    # "monitor": {
    #    "@index": "erka",
    #    ......
    # }
    query = alarm_set["monitor"].copy()
    query["@subProject"] = alarm_set["@subProject"]
    query["@create"] = {"$lte": last_str}

    # 查询表格名称。暂时只有时效性表格
    db_table = {
        "timely": custom_index.data_db.TimelyData(),
    }
    if alarm_set["db"] in db_table:
        db = db_table[alarm_set["db"]]
    else:
        logging.warning("db(%s) is not in db_table" % alarm_set["db"])
        return False

    try:
        # 去掉_id字段，按照@create倒序排列，取第一个。即获取计算时间之前离的最近的一个时间点的数据
        last_data = db.find_one(filter=query, projection={"_id": 0}, sort=[("@create", -1)])
        if last_data:
            logging.info("query: %s" % json.dumps(query, ensure_ascii=False))
            logging.info(json.dumps(last_data, ensure_ascii=False))
            # 以百分比的方式，计算的是百分比
            if condition["percent"]:
                value = float(alarm_set["@value"]-last_data["@value"])/last_data["@value"]
            # 普通的方式，只计算差值
            else:
                value = alarm_set["@value"]-last_data["@value"]
            # 计算完后，调用绝对判断的逻辑
            ret = judge_absolute(condition, value)
            update_json = {
                "alarm": False,
                "last_value": last_data["@value"],
                "last_time": last_data["@create"],
                "diff_value": value,
            }
            if ret:
                update_json["alarm"] = True
            condition.update(update_json)
            return ret
        else:
            logging.warning("query no result\n%s" % json.dumps(query, ensure_ascii=False))
            return False
    except:
        logging.exception("query error")
        logging.info("query= %s" % json.dumps(query,ensure_ascii=False))


def alert_user(alarm_set):
    u"""
    "indicator": {
        "name": "测试指标",
        "id": "pex",
        "value": 50,
        "create": "2016-09-26 14:23",
        "dimension": {

        }
    },
    "flag": True,
    "condition": {
        "type": "absolute",
        "percent": True,
        "operator": "<",
        "value": 40,
        "time": {
            "unit": "day",
            "num": 1
        }
    },
    "project": {
        "sub_project_id": 13,
        "project_name": "项目",
        "sub_project_name": "子项目",
        "page_id": 13
    },
    :param alarm_set:
    :return:
    """
    # 数据结构重组
    db = lib.mysql_db.BaseMysqlDb()
    # indicator 字段
    sql = "select `source_name`, `name` from `rawdata_indicator`" \
          " where `sub_project_id`=%s and `source_name`='%s' and `mark_del`=0"\
          % (alarm_set["@subProject"], alarm_set["monitor"]["@index"])
    logging.info(sql)
    db.cur.execute(sql)
    data = db.cur.fetchone()
    dimension = alarm_set["monitor"].copy()
    del dimension["@index"]
    if data:
        alarm_set["indicator"] = {
            "name": data[1],
            "id": data[0],
            "value": alarm_set["@value"],
            "create": alarm_set["@create"],
            "dimension": dimension,
        }
    else:
        return
    # condition 不用重组
    # project 信息重组
    sql = "select `permit_project`.`name`, `permit_subproject`.`name`" \
          " from `permit_subproject` inner join `permit_project`" \
          " on `permit_project`.`id`=`permit_subproject`.`project_id`" \
          " where `permit_subproject`.`id`=%s" % alarm_set["@subProject"]
    logging.info(sql)
    db.cur.execute(sql)
    data = db.cur.fetchone()
    if data:
        alarm_set["project"] = {
            "sub_project_id": alarm_set["@subProject"],
            "project_name": data[0],
            "sub_project_name": data[1]
        }
    else:
        return
    # 页面链接
    sql = "select perform_page.id" \
          " from perform_page inner join perform_page_group" \
          " on perform_page.page_group_id=perform_page_group.id" \
          " where perform_page_group.sub_project_id=%s and perform_page_group.mark_del=0"\
          % alarm_set["@subProject"]
    logging.info(sql)
    db.cur.execute(sql)
    data = db.cur.fetchone()
    if data:
        alarm_set["project"]["page_id"] = data[0]

    # email发送分支
    if "email" in alarm_set["alert"]["channel"]:
        logging.info("sending email")
        sender.send_remind_email(alarm_set)


def test_main(alarm_item):
    """
    测试的程序入口，不走redis
    :param alarm_item:
    :return:
    """
    try:
        alarm_set = json.loads(alarm_item)
        print alarm_set
        return alarm_check(alarm_set)
    except:
        logging.exception(u"处理失败")


def test():
    print "alarm test"
    # 设置60，< , 计算值60, 不报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 60}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 60, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == False
    # 设置60，< , 计算值59, 报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 60}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 59, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == True
    # 设置60，< , 计算值59-44, 报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": false, \"type\": \"relative\", \"operator\": \"<\", \"value\": 60, \"time\": {\"unit\": \"minute\", \"num\": 5}}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:27\", \"db\": \"timely\", \"@value\": 59, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == True
    # 设置60，< , 计算值59-44, 百分比,报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"relative\", \"operator\": \">\", \"value\": 0.2, \"time\": {\"unit\": \"minute\", \"num\": 5}}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:27\", \"db\": \"timely\", \"@value\": 59, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == True
    # 设置60，< , 计算值59-44, 百分比,不报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"relative\", \"operator\": \">\", \"value\": 0.2, \"time\": {\"unit\": \"week\", \"num\": 5}}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:27\", \"db\": \"timely\", \"@value\": 59, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == False

    # and 条件
    # 设置30<x<100 , 计算值60, 报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 100}, {\"percent\": true, \"type\": \"absolute\", \"operator\": \">\", \"value\": 30}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 60, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == True
    # 设置70<x<100 , 计算值60, 不报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 100}, {\"percent\": true, \"type\": \"absolute\", \"operator\": \">\", \"value\": 70}], \"logic\": \"and\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 60, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == False

    # or 条件
    # 设置x<30  or x>100 , 计算值60, 不报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 30}, {\"percent\": true, \"type\": \"absolute\", \"operator\": \">\", \"value\": 100}], \"logic\": \"or\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 60, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == False
    # 设置x<30  or x>100 , 计算值20, 报警
    alarm_item = "{\"monitor\": {\"@index\": \"erka\"}, \"alarm\": {\"condition\": [{\"percent\": true, \"type\": \"absolute\", \"operator\": \"<\", \"value\": 30}, {\"percent\": true, \"type\": \"absolute\", \"operator\": \">\", \"value\": 100}], \"logic\": \"or\"}, \"@create\": \"2016-09-23 17:23\", \"db\": \"timely\", \"@value\": 20, \"alert\": {\"receiver_group\": \"psqa-kgb\", \"channel\": [\"sms\", \"hi\", \"email\"], \"receiver\": \"xulei12;yangxiaotong\"}, \"@subProject\": 13}"
    assert test_main(alarm_item) == True
