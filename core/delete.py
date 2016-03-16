# coding=utf-8
import time
import datetime
import os
import shutil
import pymongo
import re

from conf import conf

def clear_data_files(date):
	dir_path = os.path.join(conf.DATA_DIR,"midpage")
	if os.path.exists(dir_path):
		files = os.listdir(dir_path)
		for file in files:
			if file <= date:
				shutil.rmtree(os.path.join(dir_path,file),True)

def clear_db(date):
	conn = pymongo.MongoClient(conf.MONGO_HOST, conf.MONGO_PORT)
	db = conn[conf.MONGO_DB]
	collections = db.collection_names(False)
	reg = re.compile(r'^datelog_(?P<db_time>[0-9]+)$')
	for coll in collections:
		match = reg.match(coll)
		if(match):
			if(match.group('db_time') <= date):
				db.drop_collection(coll)

def delete(date_limit): 
	date = datetime.datetime.now()-datetime.timedelta(days=int(date_limit))
	date_str = date.strftime("%Y%m%d")
	clear_data_files(date_str)
	clear_db(date_str)