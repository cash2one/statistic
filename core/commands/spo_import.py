# coding=utf-8
import time

from core.management.base import BaseCommand
from script import spo_import

class Command(BaseCommand):
    u"""
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        assert len(args) == 4
        assert args[2] in ("pc", "wise")
        date_format = True
        try:
            time.strptime(args[3], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, side, date, *args):
        u"""params:
    side  pc/wise
    date  %Y%m%d格式日期
        """    
        spo_import.main(side, date)

