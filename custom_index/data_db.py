# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
#
"""
文件说明：

File   : data_db.py

Authors: yangxiaotong@baidu.com
Date   : 2016-4-30
Comment:
"""
# 标准库

# 第三方库

# 自有库
from lib import mongo_db


class OriginalData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "original_data"


class DailySummaryData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "daily_summary_data"


class TimelyData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "timely_data"


class TimelyDateLatest(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "timely_data_latest"


class UserPortrait(mongo_db.BaseMongoFontDb):
    """
    mongodb数据库，用户画像表
    """
    COLLECTION_NAME = "user_portrait"


class UserPath(mongo_db.BaseMongoFontDb):
    """
    mongodb数据库，用户lujing 表
    """
    COLLECTION_NAME = "user_path"


class DetailData(mongo_db.BaseMongoFontDb):
    def __init__(self, date):
        month = date[:-2]
        self.COLLECTION_NAME = "detail_data_%s" % month
        super(DetailData, self).__init__()
