#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
# 
"""
文件说明：

File   : process_timely_data.py

Authors: xulei12@baidu.com
Date   : 2016/10/26
Comment: 
"""
# 系统库
import copy
import json
import time
import hashlib
import logging
# 第三方库

# 自有库
import data_db
import core.kgdc_redis
import alarm.alarm_db


def main():
    """
    导入时效性指标入口。
    常驻进程，死循环。
    :return:
    """
    rs = core.kgdc_redis.KgdcRedis()

    # 死循环从redis队列获取报警处理消息。
    # 无尽等待，无消息为阻塞态
    while 1:
        data = rs.pop_timely_data()
        if not data:
            continue
        try:
            json_data = json.loads(data)
            timely_data_process(json_data)
        except:
            logging.error("处理失败:\n %s" % type(json_data))
            logging.error(json_data)
            logging.exception(u"处理失败")


def timely_data_process(json_data):
    """
    根据路径不同分发任务处理
    :param json_data:
    :return:
    """
    if "@from" not in json_data:
        return
    if json_data["@from"] == "api":
        api_process(json_data)


def api_process(json_data):
    """
    api推送过来数据处理流程
    :param json_data:
    :return:
    """
    create = json_data["@create"]
    # 时间格式校验。必须为 年-月-日 时:分:秒 或者不带秒的格式
    # 最后统一转换为不带秒的格式
    try:
        create_ts = time.strptime(create, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            create_ts = time.strptime(create, "%Y-%m-%d %H:%M")
        except:
            logging.warning("create format error")
            return
    create = time.strftime("%Y-%m-%d %H:%M", create_ts)
    sub_project_id = int(json_data["@subProject"])
    task = int(json_data["@task"])
    success_nums = 0
    failed_nums = 0

    timely_data = data_db.TimelyData()
    # 获取当前subproject下所有的报警设置
    sub_project_alarm_set = get_alarm_set_by_sub_project(sub_project_id)
    for item in json_data["data"]:
        if "@index" not in item or "@value" not in item:
            continue

        item["@subProject"] = sub_project_id
        item["@create"] = create
        item["@task"] = task
        # 用于更新时效性指标库
        insert_query = item.copy()
        remove_keys = ["@task", "@time", "@value"]
        for key in remove_keys:
            if key in insert_query:
                del insert_query[key]
        try:
            # 更新最新指标库
            update_latest_data(item)

            # 分条更新时效性数据库。防止重复写数据。
            logging.info("insert timely_data")
            logging.info(item)
            timely_data.update(insert_query, item, True)
            success_nums += 1

            # 检查这一条指标的更新是否有对应的报警项需要处理。如果有，就送入redis的处理队列
            push_alarm_data(item, sub_project_alarm_set)
            # md5_key = calculate_md5_key(alarm_query)
            # if md5_key in sub_project_alarm_set:
            #     redis_item = copy.deepcopy(sub_project_alarm_set[md5_key])
            #     update_dict = {
            #         "@value": item["@value"],
            #         "@create": item["@create"],
            #         "db": "timely"
            #     }
            #     redis_item.update(update_dict)
            #     redis_item = json.dumps(redis_item, ensure_ascii=False)
            #     rs.push_alarm(redis_item)
        except:
            logging.error("write to mongodb error,\n%s" % item)
            failed_nums += 1
            continue
    logging.info("处理成功: %s, 失败：%s" % (success_nums, failed_nums))


def update_latest_data(json_data):
    """
    更新最新指标库
    :param json_data:  要插入的数据
    :return:
    """
    check_keys = ["@subProject", "@create", "@value"]
    for key in check_keys:
        if key not in json_data:
            return

    timely_data_latest = data_db.TimelyDateLatest()
    remove_keys = ["@task", "@time", "@create", "@from", "@value"]
    update_query = copy.deepcopy(json_data)
    # 此处有坑。如果不复制一份。
    # 在执行了insert操作后。json_data中会带上_id字段，造成插入另一个表格失败
    insert_data = copy.deepcopy(json_data)
    for key in remove_keys:
        if key in update_query:
            del update_query[key]
    ret = timely_data_latest.find(filter=update_query,
                                  projection={"_id": 0},
                                  sort=[("@create", -1)])
    ret = list(ret)
    # 之前没有该指标，就直接插入
    if len(ret) == 0:
        logging.info("无指标，更新latest表")
        timely_data_latest.insert(insert_data)
    # 否则，如果已存储指标比现在的旧，更新
    elif len(ret) == 1:
        if ret[0]["@create"] == json_data["@create"]:
            logging.info("有一条指标，但是时间一样，覆盖")
            timely_data_latest.update(update_query, json_data, False)
        else:
            logging.info("有一条指标，但是时间不一样，插入")
            timely_data_latest.insert(insert_data)
    elif len(ret) == 2:
        if json_data["@create"] > ret[0]["@create"]:
            logging.info("有两条条指标，数据>最新指标还新，覆盖最旧的一条")
            update_query["@create"] = ret[1]["@create"]
            timely_data_latest.update(update_query, json_data, False)
        elif json_data["@create"] == ret[0]["@create"]:
            logging.info("有两条条指标，数据==最新的数据，覆盖最新的一条")
            update_query["@create"] = ret[0]["@create"]
            timely_data_latest.update(update_query, json_data, False)
        elif json_data["@create"] >= ret[1]["@create"]:
            logging.info("有两条条指标，数据>=最旧的数据一致，覆盖最旧的一条")
            update_query["@create"] = ret[1]["@create"]
            timely_data_latest.update(update_query, json_data, False)
        else:
            logging.info("有两条条指标，数据<最旧的数据，不操作")
            pass
    else:
        logging.error("库中数据有误，超过2条。清空并更新")
        timely_data_latest.remove(update_query)
        timely_data_latest.insert(json_data)


def push_alarm_data(json_data, sub_project_alarm_set):
    """
    检查是否有报警配置，并推入报警处理队列
    :param json_data: 数据
    :param sub_project_alarm_set:
    list结构，该subProject所有的报警配置， 由get_alarm_set_by_sub_project计算而来
    :return:
    """
    # 用于查询该指标的更新是否有对应的报警设置,不带@subProject @value
    alarm_query = json_data.copy()
    # 此处需考虑维度的问题。因此采用减法，去掉不用的key进行update
    remove_keys = ["@subProject", "@task", "@time", "@create", "@value"]
    for key in remove_keys:
        if key in alarm_query:
            del alarm_query[key]
    md5_key = calculate_md5_key(alarm_query)
    rs = core.kgdc_redis.KgdcRedis()
    if md5_key in sub_project_alarm_set:
        logging.info("需要处理报警，推入处理队列")
        redis_item = copy.deepcopy(sub_project_alarm_set[md5_key])
        update_dict = {
            "@value": json_data["@value"],
            "@create": json_data["@create"],
            "db": "timely"
        }
        redis_item.update(update_dict)
        redis_item = json.dumps(redis_item, ensure_ascii=False)
        rs.push_alarm(redis_item)


def get_alarm_set_by_sub_project(sub_project_id):
    """
    一次性获取一个subProject的报警配置
    该操作耗时较多，因此在一任务里只运行一次
    key为按规则计算的md5值，
    value为配置
    :param sub_project_id:
    :return:
    """
    if type(sub_project_id) != int and type(sub_project_id) != long:
        return []
    alarm_set = alarm.alarm_db.AlarmSetDb()
    data = alarm_set.find(filter={"@subProject": sub_project_id},
                          projection={"_id": 0})

    ret = dict()
    for item in list(data):
        try:
            md5_key = calculate_md5_key(item["monitor"])
            ret[md5_key] = item
        except:
            logging.exception("translate alarm into md5 key error")
            continue
    return ret


def calculate_md5_key(monitor):
    """
    根据报警设置中的monitor字段计算生成新的key，用于查询
    :param monitor:
    :return:
    """
    monitor = monitor.items()
    monitor.sort()
    hs = hashlib.md5()
    hs.update(str(monitor))
    return hs.hexdigest().decode("utf-8")
