# coding=utf-8
from lib import mongo_db

class OriginalData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "original_data"


class DailySummaryData(mongo_db.BaseMongoFontDb):
    COLLECTION_NAME = "daily_summary_data"


class DetailData(mongo_db.BaseMongoFontDb):
    def __init__(self, date):
        month = date[:-2]
        self.COLLECTION_NAME = "detail_data_%s" % month
        super(DetailData, self).__init__()
