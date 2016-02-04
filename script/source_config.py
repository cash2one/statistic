# coding=utf-8

PC_SIDE = 'pc'
WISE_SIDE = 'wise'

SIDE_NAME = {
	PC_SIDE: 'pc',
	WISE_SIDE: 'wise'
}

SPO_SRC = {
    PC_SIDE: {
        "srcid": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/pc_query/spo/${DATE}",
        "pv": "ftp://cq01-testing-ps7163.cq01.baidu.com/home/work/kgdc-statist/kgdc-statist/data/sePc/pv.${DATE}"
    },
    WISE_SIDE: {
        "srcid": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/wise_query/wise_spo/${DATE}",
        "pv": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/wise_query/total/${DATE}"
    }
}

SPO_KPI = {
    "kpi": "ftp://cq01-testing-ps7163.cq01.baidu.com/home/work/kgdc-statist/kgdc-statist/data/kpi/spo"
}




