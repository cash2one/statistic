#coding=utf-8
import os
import sys
import time

from lib import bdtj_api as bdtj
from conf import conf
import source_config
import db
from request import Request
import time

username = 'kgb-dict'
password = 'kgb-dict'
token = '71f6e850ba11342661e17b2bb1987545'

def import_data(date, delta = 1):
    '''
    导入百度统计上字词产品相关:
        -- module_sla: 模块稳定性
        -- fe_sla: 前端交互稳定性
    '''
    start_time = time.mktime(time.strptime(date, "%Y%m%d"))
    
    obj = bdtj.BaiduTongjiApi(username, token)
    obj.pre_login()    


def main(date):
    try:
        time.strptime(date, "%Y%m%d")
    except:
        tools.log(u"日期格式错误:%s" % date)
        tools.log(u"日期必须是%Y%m%d格式")
        return
    import_data(date)
