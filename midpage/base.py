# coding=utf-8
import os

from midpage import midpagedb

class MidpageProduct(object):
    def __init__(self, date):
    	self.date = date
        self.log_db = midpagedb.DateLogDb()
        self.log_collection = self.log_db.get_collection()

    def statist(self):
        raise NotImplementedError()

    def save_result(self, result):
        raise NotImplementedError()

    def run(self):
        self.save_result(self.statist())