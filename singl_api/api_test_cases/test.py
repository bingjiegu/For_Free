
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO

from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import create_schedulers
import unittest

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])



# class remove_schedulers(unittest.TestCase):
#     def test_case01(self):
scheduler_id1 = create_schedulers()
time.sleep(2)
scheduler_id2 = create_schedulers()
print(scheduler_id1, scheduler_id2)
# remove_list_url = "%s/api/schedulers/removeList" % (MY_LOGIN_INFO["HOST"])
# data = [scheduler_id1, scheduler_id2]
# res = requests.post(url=remove_list_url, headers=get_headers(), data=data)
# print(res.status_code, res.text)





