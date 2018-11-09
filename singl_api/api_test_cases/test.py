from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_new_schedulers
import unittest
import requests
import json
from basic_info.format_res import dict_res, get_time
from basic_info.url_info import *

from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO,scheduler_name
from basic_info.Open_DB import MYSQL
import time


# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


"""该类用来进行脚本的调试"""
class delete_schedulers(unittest.TestCase):

    def test_case01(self):
        scheduler1 = get_new_schedulers()
        time.sleep(3)
        scheduler2 = get_new_schedulers()
        # data = ["ba2f07ea-8bd5-47b1-b3b3-8756bceb608d","6d74d437-9d31-4cbb-a87b-f71e34e7b616"]
        res = requests.post(url=delete_schedulers_url, headers=get_headers(), json=[scheduler1, scheduler2])
        print(res.status_code, res.text)


