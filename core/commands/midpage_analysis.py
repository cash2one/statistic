# coding=utf-8
import time

from core.management.base import BaseCommand
from midpage import analysis

class Command(BaseCommand):
    u"""
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        assert len(args) >= 3
        date_format = True
        try:
            time.strptime(args[2], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, date, sources=None, *args):
        u"""params:
    date  %Y%m%d格式日期
    sources 分来源进行统计，逗号分隔，可选，默认全部
        """
        analysis.main(date, sources)

