# coding=utf-8
import time

from core.management.base import BaseCommand
from custom_index import scheduler

class Command(BaseCommand):

    def assert_argv(self, *args):
        pass

    def handle(self, *args):
        u""""""
        scheduler.main()
