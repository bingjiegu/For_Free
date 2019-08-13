from util.timestamp_13 import get_now_time
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG


class CleanData(object):
    def __init__(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

    def clean_datasource_test_data(self):
        today = get_now_time()[1]
        today_dss_sql = "delete from merce_dss where creator = 'admin' and create_time like '%s%%' and name not in (select name from merce_dss where name like '%测试用%')" % today
        print(today_dss_sql)
        self.ms.ExecuNoQuery(today_dss_sql)
