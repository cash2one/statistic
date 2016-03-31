# coding=utf-8
import time

from core.management.base import BaseCommand
from script import bdtj_dict_import

class Command(BaseCommand):
    u"""
    入库字词百度统计指标
    """

    def assert_argv(self, *args):
        assert len(args) == 3
        date_format = True
        try:
            time.strptime(args[2], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, date, *args):
        u"""params:
        date  %Y%m%d格式日期
        """    
        bdtj_dict_import.main(date)

