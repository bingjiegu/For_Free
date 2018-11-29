from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, owner, dataset_resource, schema_resource, MY_LOGIN_INFO, tenant_id
from basic_info.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


# 该脚本用来校验create flow接口的参数
class ForCreateFlow(unittest.TestCase):
    """create flow api参数校验"""
    from basic_info.url_info import create_flow_url

    def test_case01(self):
        """创建flow时， name为空"""
        # flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": "", "flowType": "streamflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        # response_text = json.loads(res.text)
        # print(res.status_code, res.text)
        err = json.loads(res.text)
        # print(type(err), err, )
        err_dict = json.loads(err["err"])
        print("err_dict", err_dict)
        err_code = int(err_dict["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建flow时， name为空时的err_code不正确')
        time.sleep(3)

    def test_case02(self):
        """创建flow时， name缺失"""
        # flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"flowType": "streamflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        # response_text = json.loads(res.text)
        # print(res.status_code, res.text)
        err = json.loads(res.text)
        # print(type(err), err, )
        err_dict = json.loads(err["err"])
        err_code = int(err_dict["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建flow时， name为空时的err_code不正确')
        time.sleep(3)

    def test_case03(self):
        """创建flow时， flow type为空"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": flow_name, "flowType": "", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        # response_text = json.loads(res.text)
        # print(res.status_code, res.text)
        err = json.loads(res.text)
        # print(type(err), err, )
        err_dict = json.loads(err["err"])
        err_code = int(err_dict["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建flow时， name为空时的err_code不正确')
        time.sleep(3)

    def test_case04(self):
        """创建flow时， flow type非法"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": flow_name, "flowType": "ttttt", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        # response_text = json.loads(res.text)
        # print(res.status_code, res.text)
        err = json.loads(res.text)
        # print(type(err), err, )
        err_dict = json.loads(err["err"])
        err_code = int(err_dict["list"][0]["code"])
        self.assertEqual(err_code, 903, '创建flow时， name为空时的err_code不正确')
        time.sleep(3)

    def test_case05(self):
        """创建flow时， flow type缺失"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": flow_name, "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        # response_text = json.loads(res.text)
        # print(res.status_code, res.text)
        err = json.loads(res.text)
        # print(type(err), err, )
        err_dict = json.loads(err["err"])
        err_code = int(err_dict["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建flow时， name为空时的err_code不正确')
        time.sleep(3)

    # def test_case09(self):
    #     """创建flow时name重复"""
    #     # 查询最近创建的flow，取出name
    #     SQL = 'select name from merce_flow order by create_time desc limit 1'
    #     flow_info = ms.ExecuQuery(SQL)
    #     flow_name = flow_info[0][0]
    #     print(flow_name, type(flow_name))
    #     # flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'flow'
    #     headers = headers_info()[3]
    #     data = {"name": flow_name, "flowType": "dataflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
    #             "steps": [], "links": []}
    #     res = requests.post(url=Create_flow.url, headers=headers, data=json.dumps(data))
    #     print(res.status_code, json.loads(res.text))


class ForCreateDataSet(unittest.TestCase):
    """create dataset api参数校验"""
    from basic_info.url_info import create_dataset_url
    storage = get_datasource()
    storageConfigurations = {"format": "csv", "path": "/tmp/gubingjie", "relativePath": "/tmp/gubingjie",
                             "recursive": "false", "header": "false", "separator": ",", "quoteChar": "\"",
                             "escapeChar": "\\"}

    dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'

    def test_case04(self):
        """--创建HDFS dataset, name参数值为空--"""
        schema_info = schema()
        data = {"name": "", "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, res.text)
        # 取得res.text中的code, 用来做断言
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建HDFS dataset, name参数值为空时err_code错误')

    def test_case05(self):
        """创建HDFS dataset, 缺少name参数"""
        schema_info = schema()
        data = {"schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建HDFS dataset, 缺少name参数时err_code不正确')

    def test_case06(self):
        """创建dataset, resource参数为空"""
        schema_info = schema()
        data = {"name": self.dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42",
                "resource": {}}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, resource参数为空时err_code不正确')

    def test_case07(self):
        """创建dataset, 缺失resource参数"""
        schema_info = schema()
        data = {"name": self.dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42"}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 900, '创建dataset, 缺失resource参数时err_code不正确')

    def test_case08(self):
        """创建dataset, resource参数错误, resource 为dataset的resource"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": schema_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case08', res.text)
        err = json.loads(res.text)
        err_message = err["err"]
        err_message = err_message.strip()
        self.assertEqual(err_message, 'dataset resource id is wrong', '创建dataset, resource参数错误时err message不正确')

    def test_case09(self):
        """创建dataset, storageConfigurations的值为空"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": {}, "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case08', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storageConfigurations为空时err_code不正确')

    def test_case10(self):
        """创建dataset, storageConfigurations缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                 "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storageConfigurations缺失时err_code不正确')

    def test_case11(self):
        """创建dataset, schema缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        data = {"name": self.dataset_name,
                "storage": "HDFS",
                "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 900, '创建dataset, schema缺失时err_code不正确')

    def test_case12(self):
        """--创建dataset, schema值为空--"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        data = {"name": self.dataset_name,
                "schema": {},
                "storage": "HDFS",
                "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        self.assertEqual(res.status_code, 500, 'Schema value 为空时status_code不正确')

    def test_case13(self):
        """创建dataset, storage非法"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name,
                "schema": schema_info,
                "storage": "HDF",
                "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        try:
            err = json.loads(res.text)
            err = json.loads(err["err"])
            err_code = int(err["list"][0]["code"])
            self.assertEqual(err_code, 903, '创建dataset, storage非法时err_code不正确')
        except Exception as e:
            print('测试用例--创建dataset, storage非法--执行失败')

    def test_case14(self):
        """创建dataset, storage为空"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name,
                "schema": schema_info,
                "storage": "",
                "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storage为空时err_code不正确')

    def test_case15(self):
        """创建dataset, storage缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": self.dataset_name,
                "schema": schema_info,
                "expiredPeriod": 0,
                "storageConfigurations": self.storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, schema缺失时err_code不正确')


class ForCreateSchema(unittest.TestCase):
    """create schema api参数校验"""
    from basic_info.url_info import create_schema_url
    schema_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schema'
    # 创建时name重复

    def test_case02(self):
        """创建schema时name重复"""
        # 查找最后一个schema的name，作为该case的name
        query = 'select name from merce_schema order by create_time limit 1'
        schema_name = ms.ExecuQuery(query)
        schema_name = schema_name[0]["name"]
        data = {"name": schema_name, "fields": [{"name": "id", "type": "int"}],
                "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
        text = json.loads(res.text)
        message = text["err"]
        # print(res.status_code, res.text)
        self.assertEqual(res.status_code, 501, "错误message为%s" % message)

    def test_case03(self):
        """创建schema时name参数的值为空"""
        data = {"name": "", "fields": [{"name": "id", "type": "int"}],
                "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
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
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        text_err = json.loads(text['err'])
        # print(text_err)
        text_err_code = int(text_err["list"][0]["code"])
        # print(text_err_code)
        message = text_err["list"][0]["message"]
        # print(type(message))
        self.assertEqual(text_err_code, 902, "缺失name参数时的错误码不正确")

    def test_case05(self):
        """创建schema时field参数值为空"""

        data = {"name": ForCreateSchema.schema_name, "fields": [],
                "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, res.text)
        # 获取返回的错误内容中的message
        text = json.loads(res.text)
        text_err = json.loads(text["err"])
        text_err_code = int(text_err["list"][0]["code"])
        message = text_err["list"][0]["message"]
        self.assertEqual(text_err_code, 903, "错误message为%s" % message)

    def test_case06(self):
        """创建schema时缺失fields参数"""
        data = {"name": ForCreateSchema.schema_name, "resource": {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
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
        data = {"name": ForCreateSchema.schema_name, "fields": [{"name": "id", "type": "int"}], "resource": {}}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
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
        data = {"name": ForCreateSchema.schema_name, "fields": [{"name": "id", "type": "int"}]}
        res = requests.post(url=self.create_schema_url, headers=get_headers(), data=json.dumps(data))
        # id = json.loads(res.text)
        # print(res.status_code, res.text)
        text = json.loads(res.text)
        # print(text)
        # print(type(text))
        text = json.loads(text["err"])
        code = int(text["list"][0]["code"])
        self.assertEqual(code, 900, "缺少resource参数时的code不正确")



