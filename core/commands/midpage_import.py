# coding=utf-8
import time
import re

from core.management.base import BaseCommand
from script import midpage_import

class Command(BaseCommand):
    u"""
    创建每个月的数据库
    """

    def assert_argv(self, *args):
        assert len(args) == 4
        assert re.compile('\d+').match(args[2])
        date_format = True
        try:
            time.strptime(args[3], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, product_id, date, *args):
        u"""params:
    product_id  数据库中的产品id
    date  %Y%m%d格式日期
        """    
        midpage_import.main(product_id, date)

