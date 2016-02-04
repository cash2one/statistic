# coding=utf-8
import time

from core.management.base import BaseCommand
from script import spo_kpi_import

class Command(BaseCommand):
    u"""
    入库问答kpi指标数据
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
        spo_kpi_import.main(date)

