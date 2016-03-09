# coding=utf-8
import time

from core.management.base import BaseCommand
from custom_index import import_original_data

class Command(BaseCommand):

    def assert_argv(self, *args):
        assert len(args) == 4
        try:
            task_id = int(args[2])
        except:
            task_id = 0
        assert task_id

        date_format = True
        try:
            time.strptime(args[3], "%Y%m%d")
        except:
            date_format = False
        assert date_format

    def handle(self, task_id, date, *args):
        u"""params:
    task_id 导入任务id
    date  %Y%m%d格式日期
        """
        import_original_data.main(task_id, date)

