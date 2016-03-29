# coding=utf-8
import json
import datetime

from lib import mysql_db
from lib import error

class CustomIndexTask(mysql_db.BaseMysqlDb):
    def __init__(self, task_id):
        super(CustomIndexTask, self).__init__()
        self._get_task_info(task_id)

    def _get_task_info(self, task_id):
        sql = "select `id`,`name`,`owner`,`topic`,`routine`,`path`,\
            `time_delta`,`hour`,`period`,`last_run_date`, `task_type` from custom_index_task where id=%s" % task_id
        self.cur.execute(sql)
        line = self.cur.fetchone()
        if line is None:
            raise error.DataBaseError(u"task %s not find" % task_id)
        self.id = line[0]
        self.name = line[1]
        self.owner = line[2]
        self.topic = line[3]
        self.routine = True if line[4] else False
        self.path = line[5]
        self.time_delta = line[6]
        self.hour = line[7]
        self.period = line[8]
        self.last_run_date = line[9]
        self.task_type = line[10]

        if not self.period:
            self.period = {"type":"daily"}
        else:
            self.period = json.loads(self.period)

    def update_last_run_date(self, date):
        sql = "update custom_index_task set last_run_date='%s' where id=%s" %\
            (date, self.id)
        self.cur.execute(sql)
        self.conn.commit()

    def if_run_today(self):
        if self.period["type"] == "daily":
            return True
        elif self.period["type"] == "weekly":
            today = datetime.date.today()
            if today.weekday() == self.period["weekday"]:
                return True
            else:
                return False
        raise error.Error("unkown period type")

    @classmethod
    def get_routine_tasks_by_hour(cls, hour):
        conn, cur = cls._get_connect()
        sql = "select `id` from custom_index_task where routine=1 and hour=%s" % hour
        cur.execute(sql)
        lines = cur.fetchall()
        lines = [line[0] for line in lines]
        return lines

    @classmethod
    def get_unroutine_tasks(cls):
        conn, cur = cls._get_connect()
        sql = "select `id` from custom_index_task where routine=0 and `task_type`='index'"
        cur.execute(sql)
        lines = cur.fetchall()
        lines = [line[0] for line in lines]
        return lines