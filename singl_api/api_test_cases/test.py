
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO

from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_flows, flow_id
import unittest
import json


# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


# class update_scheduler(unittest.TestCase):
#     """测试update计划接口, update name"""
#     def test_case01(self):
#         # from basic_info.url_info import update_scheduler_url
#         update_scheduler_url = "%s/api/schedulers/a1bd03e7-52bc-4816-a02e-f740f49a3e3a" % (MY_LOGIN_INFO["HOST"])
#         scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'update_schedulers'
#         data = {"name": scheduler_name}
#         res = requests.put(url=update_scheduler_url, headers=get_headers(), json=data)
#         print(res.status_code, res.text)
#         # self.assertEqual(res.status_code, 201, '创建单次执行的scheduler失败')







