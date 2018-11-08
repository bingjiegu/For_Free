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

    # 创建时name重复
    def test_case02(self):
        """创建schema时name重复"""
        # 查找最后一个schema的name，作为该case的name
        query = 'select name from merce_schema order by create_time limit 1'
        schema_name = ms.ExecuQuery(query)
        schema_name = schema_name[0][0]
        data = {"name": schema_name, "fields": [{"name": "id", "type": "int"}], "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        text = json.loads(res.text)
        message = text["err"]
        # print(res.status_code, res.text)
        self.assertEqual(res.status_code, 501, "错误message为%s" % message)

    def test_case03(self):
        """创建schema时name参数的值为空"""
        data = {"name": "", "fields": [{"name": "id", "type": "int"}], "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, 'res.text', res.text)
        text = json.loads(res.text)
        # print(text)
        text_err = json.loads(text['err'])
        text_err_code = int(text_err["list"][0]["code"])
        message = text_err["list"][0]["message"]
        # print(text_err_code,  message)
        # print("message", message)
        self.assertEqual(text_err_code, 902, "错误message为%s" % message)

    def test_case04(self):
        """创建schema时缺失name参数"""
        data = {"fields": [{"name": "id", "type": "int"}], "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        text_err = json.loads(text['err'])
        # print(text_err)
        text_err_code = int(text_err["list"][0]["code"])
        # print(text_err_code)
        message = text_err["list"][0]["message"]
        # print(type(message))
        self.assertEqual(text_err_code, 902,  "缺失name参数时的错误码不正确")

    def test_case05(self):
        """创建schema时field参数值为空"""

        data = {"name": Create_schema.schema_name, "fields": [], "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, res.text)
        # 获取返回的错误内容中的message
        text = json.loads(res.text)
        text_err = json.loads(text["err"])
        text_err_code = int(text_err["list"][0]["code"])
        message = text_err["list"][0]["message"]
        self.assertEqual(text_err_code, 903, "错误message为%s" % message)

    def test_case06(self):
        """创建schema时缺失fields参数"""
        data = {"name": Create_schema.schema_name, "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        text_err = json.loads(text["err"])
        text_err_code = int(text_err["list"][0]["code"])
        message = text_err["list"][0]["message"]
        # print(text_err_code)
        # print(message)
        self.assertEqual(text_err_code, 900, "缺失field参数时的错误码不正确")


    def test_case07(self):
        """创建schema时resource参数为空"""
        data = {"name": Create_schema.schema_name, "fields": [{"name": "id", "type": "int"}], "resource": {}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        text_err = json.loads(text["err"])
        text_err_code = int(text_err["list"][0]["code"])
        message = text_err["list"][0]["message"]
        # print(text_err_code)
        # print(message)
        self.assertEqual(text_err_code, 902, "resource参数为空时的error_code不正确")

    def test_case09(self):
        """创建schema时缺少resource参数"""
        data = {"name": Create_schema.schema_name, "fields": [{"name": "id", "type": "int"}]}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        # print(text)
        # print(type(text))
        text = json.loads(text["err"])
        code = int(text["list"][0]["code"])
        self.assertEqual(code, 900, "缺少resource参数时的code不正确")




# if __name__ == '__main__':
#     unittest.main()






