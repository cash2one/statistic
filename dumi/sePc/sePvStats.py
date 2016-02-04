#-*- coding: utf-8 -*-
import getopt
import sys,os
import json
import subprocess
import shutil
import time
import math
from optparse import OptionParser

class sePvStats(object):
    def __init__(self, ds = 1):
        ds = int(ds)
        #self.api = "http://tc-ps-ubstest0.tc.baidu.com:8001/getdata"
        self.api = "http://cq01-test-nlp1.cq01.baidu.com:8002/getdata"
        ##获取资源号85的展现次数
        self.searchNumApi = "%s?resource=data:srcid,frequency:day,display:1,resource:85,style:0,category:pcpad,position:0,series:searchNum,statsource:ALL" % (self.api)
        ##获取资源号85的影响面
        self.searchRation = "%s?resource=data:srcid,frequency:day,display:1,resource:85,style:0,category:pcpad,position:0,series:searchRatio,statsource:ALL" % (self.api)
        self.date = time.strftime('%Y%m%d', time.localtime(time.time() - ds*86400))
        self.libs = os.path.dirname(os.path.abspath(sys.argv[0])) + "/libs"
        self.data = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0])))) + "/data/sePc"
        
    def getData(self, api):
        ##使用孔大牛开发的自动登录内网站点的脚本，注意该脚本中登录账号与密码定期维护
        cmd = "python %s/uuap.py %s" % (self.libs, api)
        print api
        try:
            strOutput = []
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            retcode = p.wait()
            for line in p.stdout:
                strOutput.append(line.strip())
            jsonResult = json.loads("".join(strOutput))
            result = jsonResult.get("html", "{}")
        except:
            sys.exit()
        obj = json.loads(result)
        return obj
    
    def extract(self, data, date):
        ##抽取指定日期的数据
        index = 0

        if data is not None:
            for i in data:
                if i[0] == date:
                    index = i[1]
                    break
        
        return index
        
    def execute(self):
        yesterday = (int(time.mktime(time.strptime(self.date, '%Y%m%d'))))*1000
        searchNum = self.extract(self.getData(self.searchNumApi), yesterday) 
        searchRatio = self.extract(self.getData(self.searchRation), yesterday)
        
        pv = 0
        resultFile = "%s/pv.%s" % (self.data, self.date)
        if searchRatio > 0:
            pv = int(math.ceil(searchNum*100/searchRatio))
        fb = open(resultFile, "w+")
        fb.write(str(pv))
        fb.close()
        print resultFile
    
if __name__ == '__main__':
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option('-s', '--ds', dest='ds', help='ds', action='store', type='string', default = None)

    (options, args) = parser.parse_args()
    ds = options.ds
    if ds is None:
        ds = 1
    sePvStats(ds).execute()
