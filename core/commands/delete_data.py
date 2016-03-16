# coding=utf-8

from core.management.base import BaseCommand
from core import delete

class Command(BaseCommand):
	u"""
	删除mongodb和data目录数据
	"""

	def assert_argv(self, *args):
		assert len(args)>=2

	def handle(self,date_limit=30,*args):
		u"""params:
    date_limit int格式，数据保留的最长天数，默认30天
		"""
		delete.delete(date_limit)
