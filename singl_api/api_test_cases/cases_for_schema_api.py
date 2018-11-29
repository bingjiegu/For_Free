from basic_info.get_auth_token import get_headers
import unittest
import requests
import json
from basic_info.data_from_db import *
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG,MY_LOGIN_INFO

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
url = '%s/api/schemas' % MY_LOGIN_INFO["HOST"]


class Create_schema(unittest.TestCase):
    """测试create schema api"""
    schema_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schema'

    # 正常创建
    def test_case01(self):
        """正常创建schema"""
        data = {"name": Create_schema.schema_name, "fields": [{"name": "id", "type": "int"}], "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        id = json.loads(res.text)
        # print(res.status_code, res.text)
        self.assertEqual(res.status_code, 201, 'schema创建失败')
        self.assertIsNotNone(res.text, '创建schema时没有返回schemaid')







