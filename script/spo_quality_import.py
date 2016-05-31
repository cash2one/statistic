# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : spo_quality_import.py

Authors: yangxiaotong@baidu.com
Date   : 2015-12-30
Comment:
"""
# 标准库
import time
import logging
# 第三方库

# 自有库
import db
from request import Request


def import_spo_data(date, delta=1):
    u"""
    导入spo质量效果数据，目前主要统计『总体』:
        -- module_sla: 模块稳定性
        -- fe_sla: 前端交互稳定性
    :param date: 
    :param delta: 
    :return: 
    """
    start_time = time.mktime(time.strptime(date, "%Y%m%d"))
    index_dict = {}
    arr = []
    index_dict['module_sla'] = {'pc': '9', 'wise': '33'}
    index_dict['fe_sla'] = {'pc': '27', 'wise': '34'}
    kgqc = {'host': 'kgqc.baidu.com', 'port': '80', 'url': "/statistics/api/getStatisticsForKgdc/"}   
    
    data = {}
    for idx in index_dict:
        '''module_sla/fe_sla'''
        for client in index_dict[idx]:
            arr.append('%s' % index_dict[idx][client])
    data['moduleId'] = ','.join(arr) 

    client = Request()
    '''回溯最近15天的数据'''
    for i in range(15, -1, -1):
        search_date = time.strftime('%Y%m%d', time.localtime(start_time - i*24*60*60))
        data['start'] = search_date
        data['end'] = search_date
        kgqc_data = client.get(kgqc['host'], kgqc['port'], kgqc['url'], data)   

        if 'data' in kgqc_data:
            for idx_key in index_dict:
                stat_db = db.SaveDataBase(search_date)
                print "remove %s data of %s..." % (idx_key, search_date)
                stat_db.clear_spo_daily_summary(search_date, [idx_key])

                for cli_key in index_dict[idx_key]:
                    value = 0
                    print cli_key
                    if index_dict[idx_key][cli_key] in kgqc_data['data'].keys():
                        kgqc_item = kgqc_data['data'][index_dict[idx_key][cli_key]]['data']
                        if kgqc_item['check_num'] > 0:
                            value = round(float(kgqc_item['check_num'] - kgqc_item['alarm_num'])/kgqc_item['check_num'], 6)

                    indata = {}
                    indata['side'] = cli_key
                    indata['product'] = u'总体'
                    indata['pid'] = 0
                    indata['index'] = idx_key
                    indata['value'] = value
                    indata['date'] = search_date
                    indata['last_modify_date'] = search_date
                    print "insert data into db"
                    ret = stat_db.save_spo_index_info([indata])


def main(date):
    try:
        time.strptime(date, "%Y%m%d")
    except:
        logging.info(u"日期格式错误:%s" % date)
        logging.info(u"日期必须是%Y%m%d格式")
        return
    import_spo_data(date)
