# coding=utf-8

PC_SIDE = 1
WISE_SIDE = 2

SIDE_NAME = {
	PC_SIDE: 'pc',
	WISE_SIDE: 'wise'
}

SPO_SRC = {
    PC_SIDE: {
        "srcid": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/pc_query/spo/${DATE}",
        "pv": "ftp://cp01-testing-ps7164.cp01.baidu.com/home/work/odp/data/app/dumi/sePc/pv.${DATE}"
    },
    WISE_SIDE: {
        "srcid": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/wise_query/wise_spo/${DATE}",
        "pv": "ftp://nj02-wd-knowledge47-ssd1l.nj02.baidu.com/home/spider/yangtianxing01/wise_query/total/${DATE}"
    }
}


