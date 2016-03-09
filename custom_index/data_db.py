# coding=utf-8

from lib import mongo_db

class OriginalData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "original_data"


class DailySummaryData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "daily_summary_data"