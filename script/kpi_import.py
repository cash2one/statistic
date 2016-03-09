#coding=utf-8
import os
import sys
import time

from lib import tools
from conf import conf
import source_config
import db

def get_pv(date):
    '''
    获取pv数据
    '''
    
def get_spo_product(date):
    '''
    获取维度信息：方向(spo)、类目、端(PC/WISE/DM NA/DM手百)
    '''
    pdict = {}
    stat_db = db.SelectDataBase()
    result = stat_db.get_spo_product()

    for i in result:
        pd = i[3]
        if not pdict.has_key(pd):
            pdict[pd] = {}
        if i[1] not in pdict[pd]:
            pdict[pd][i[1]] = {}
        pdict[pd][i[1]] = i[2].split(',')

    return pdict

def init_index_dict(index_list):    
    '''
    初始化指标索引词典
    '''
    index_dict = {}
    for key in index_list:
        index_dict[key] = {'value': 0, 'last_modify_date': 0}

    return index_dict
   

def import_spo_data(date):
    '''
    导入spo kpi:
        -- pv: 总日PV
        -- pv_influence: 搜索PV覆盖率
        -- accuracy: 结果准确率
        -- se_coverage: 需求覆盖率
    '''
    index_list = ['pv', 'pv_influence', 'accuracy', 'se_coverage', 'data_amount']
    spo_product = get_spo_product(date)
    manual_kpi = get_manual_file(date, spo_product)
    date = time.strftime("%Y-%m-%d",time.strptime(date, "%Y%m%d"))

    data = []
    if spo_product:
        for side in spo_product:
            index_dict = init_index_dict(index_list)
            for category in spo_product[side]:
                srcdict = spo_product[side]
                for val in index_list:
                    temp = {}
                    temp['side'] = side
                    temp['product'] = category
                    temp['date'] = date
                    
                    stats = None
                    if val == 'pv':
                        stats = get_pv(srcdict[category], side, date)
                    elif val == 'pv_influence':
                        stats = get_pv_influence(srcdict[category], side, date)
                    elif val == 'accuracy': 
                        if manual_kpi[side].has_key(category) and manual_kpi[side][category]['accuracy']!='-':
                            stats = {}
                            stats['value'] = float(manual_kpi[side][category]['accuracy'])
                            stats['amount_ratio'] = manual_kpi[side][category]['amount_ratio']
                            stats['last_modify_date'] = manual_kpi[side][category]['last_modify_date']
                    elif val == 'se_coverage':
                        if manual_kpi[side].has_key(category) and manual_kpi[side][category]['se_coverage']!='-':
                            stats = {}
                            stats['value'] = float(manual_kpi[side][category]['se_coverage'])
                            stats['amount_ratio'] = manual_kpi[side][category]['amount_ratio']
                            stats['last_modify_date'] = manual_kpi[side][category]['last_modify_date']
                    elif val == 'data_amount':
                        if manual_kpi[side].has_key(category) and manual_kpi[side][category]['datamount']!='-':
                            stats = {}
                            stats['value'] = float(manual_kpi[side][category]['datamount'])
                            stats['last_modify_date'] = manual_kpi[side][category]['last_modify_date']
                    temp['index'] = val
                    if stats is not None and ('value' in stats) :                       
                        temp['value'] = stats['value']
                        temp['last_modify_date'] = stats['last_modify_date']
                        if val == 'pv' or val == 'pv_influence' or val == 'data_amount':
                            index_dict[val]['value'] += stats['value']
                        elif val == 'accuracy' or val == 'se_coverage':
                            index_dict[val]['value'] += stats['value']*stats['amount_ratio']
                        if index_dict[val]['last_modify_date'] < stats['last_modify_date']:
                            index_dict[val]['last_modify_date'] = stats['last_modify_date']
                        data.append(temp)
            #总量数据
            for idx in index_dict:
                temp = {}
                temp['side'] = side
                temp['product'] = u'总体'
                temp['index'] = idx
                temp['date'] = date
                temp['value'] = index_dict[idx]['value']
                temp['last_modify_date'] = index_dict[idx]['last_modify_date']
                data.append(temp)
    
    stat_db = db.SaveDataBase(date)
    print "remove data of %s..." % (date)
    stat_db.clear_spo_daily_summary(date, index_list)
    print "insert data into db"  
    ret = stat_db.save_spo_index_info(data)

def get_pv(srcid, side, date):
    '''
    根据srcid获取资源号pv
    '''
    res = {}
    stat_db = db.SelectDataBase()
    stats = stat_db.get_spo_srcid_stat('srcid_pv', side, date, srcid) 
    if stats and stats[0][0] is not None:
        res['value'] = float(stats[0][0])
        res['last_modify_date'] = date 

    return res

def get_pv_influence(srcid, side, date):
    '''
    根据srcid获取资源号pv影响面
    '''
    res = {}
    stat_db = db.SelectDataBase()
    stats = stat_db.get_spo_srcid_stat('srcid_effect', side, date, srcid) 
    if stats and stats[0][0] is not None:
        res['value'] = float(stats[0][0])
        res['last_modify_date'] = date

    return res

def build_file_map(file):
    data = []
    with open(file) as fp:
         for line in fp:
            if line.startswith("__"):
                continue
            list = []
            line = line.rstrip("\r\n").decode("utf-8")
            list = line.split(" ")
            data.append(list)
    return data

def get_manual_file(date, spo):
    '''
    不在spo列表中的数据不计入data_amount
    '''
    result = {"pc":{}, "wise":{}, "dumi_mb":{}, "dumi_na":{}}
    damount_dict = {"pc":0, "wise":0, "dumi_mb":0, "dumi_na":0}

    path = "%s/spo.kpi" % os.path.join(conf.DATA_DIR, date)
    srcftp = source_config.SPO_KPI["kpi"]
    try:
        tools.wget(srcftp, path)
    except:
        tools.log(u"下载失败！")
        return 
    data = build_file_map(path)
    data_amount = 0

    if data:
        inidict = {}
        #计算能够与数据库中对应起来的类目数据总量
        for key in data:
            if key[1] != '-':
                if key[4] == "0":
                    inidict = result
                else:
                    inidict[key[4]] = {}
                for side in inidict:
                    if side in spo and key[0] in spo[side].keys():
                        damount_dict[side] += int(key[1])
        print damount_dict
        for key in data:
            for side in inidict:
                temp = {}
                temp['datamount'] = key[1]
                temp['accuracy'] = key[2]
                temp['se_coverage'] = key[3]
                temp['last_modify_date'] = key[5]
                temp['amount_ratio'] = 0
                if side in damount_dict and damount_dict[side] > 0 and key[1] != '-':
                    temp['amount_ratio'] = round(float(key[1])/damount_dict[side], 6)
                result[side][key[0]] = temp
    return result           

def main(date):
    try:
        time.strptime(date, "%Y%m%d")
    except:
        tools.log(u"日期格式错误:%s" % date)
        tools.log(u"日期必须是%Y%m%d格式")
        return
    import_spo_data(date)
