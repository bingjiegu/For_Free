from basic_info.get_auth_token import get_headers, get_auth_token
from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, owner, dataset_resource, schema_resource, MY_LOGIN_INFO
from basic_info.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
storageConfigurations = {"format": "csv", "path": "/tmp/gubingjie", "relativePath": "/tmp/gubingjie",
                             "recursive": "false", "header": "false", "separator": ",", "quoteChar": "\"",
                             "escapeChar": "\\"}
url = '%s/api/datasets' % MY_LOGIN_INFO["HOST"]
storage = get_datasource()

class Create_DataSet(unittest.TestCase):
    """该脚本用来测试创建dataset的场景,包含正向流和异向流"""

    def test_case01(self):
        """--正常创建DBdataset，选择已存在的schema属性为true--"""
        try:
            schema_query = 'select id from merce_schema where name = "gbj_schema"'
            schema = ms.ExecuQuery(schema_query)
        except Exception as e:
            raise e
        else:
            schema_id = {}
            schema_id["id"] = schema[0][0]
            # print(schema_id)
            data = {"name": dataset_name, "expiredPeriod": 0, "storage": "JDBC", "storageConfigurations": storage, "schema": schema_id, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
            res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))

            # print(res.status_code, res.text)
            self.assertEqual(res.status_code, 201, 'DB-dataset创建失败')
            # time.sleep(2)

    # def test_case02(self):
    #     """正常创建DB dataset，选择已存在的schema为false"""
    #     url = '%s/datasets' % Check_DataSet.host
    #     # schema = {"id": "c71b8d28-6c5b-4b9f-a470-61eda073bd6e", "name": "gbj_test_1016_city"}
    #     schema = get_schema()
    #     data = {"name": Check_DataSet.dataset_name, "expiredPeriod": 0, "storage": "JDBC", "storageConfigurations": Check_DataSet.storage, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
    #     res = requests.post(url=url, headers=Check_DataSet.headers, data=json.dumps(data))
    #     self.assertEqual(res.status_code, 201, 'DB-dataset创建失败')

    def test_case03(self):
        """--正常创建HDFSdataset--"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()  # data_from_db中schema()查询schema
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
        "storageConfigurations": storageConfigurations, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # 断言成功时响应状态码为201
        self.assertEqual(res.status_code, 201, 'HDFS dataset创建失败')
        text = json.loads(res.text)
        text = text["id"]
        try:
            query = 'select id from merce_dataset where name = "%s"' % dataset_name
            new_dataset = ms.ExecuQuery(query)
        except:
            print('没有查询到datasetname为%s的dataset' % dataset_name)
        else:
            new_dataset = new_dataset[0][0]
            # print(text, new_dataset)
            # 根据datasetname查询到dataset ID， 并和返回的text中包含的ID进行对比
            self.assertEqual(text, new_dataset, '返回的dataset id和使用该dataset的 name查询出的id不相等')

    def test_case04(self):
        """--创建HDFS dataset, name参数值为空--"""
        schema_info = schema()
        data = {"name": "", "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
                "storageConfigurations": storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建HDFS dataset, 缺少name参数时err_code不正确')

    def test_case06(self):
        """创建dataset, resource参数为空"""
        schema_info = schema()
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42",
                "resource": {}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, resource参数为空时err_code不正确')

    def test_case07(self):
        """创建dataset, 缺失resource参数"""
        schema_info = schema()
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42"}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 900, '创建dataset, 缺失resource参数时err_code不正确')

    def test_case08(self):
        """创建dataset, resource参数错误, resource 为dataset的resource"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations, "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": schema_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case08', res.text)
        err = json.loads(res.text)
        err_message = err["err"]
        err_message = err_message.strip()
        self.assertEqual(err_message, 'dataset resource id is wrong', '创建dataset, resource参数错误时err message不正确')

    def test_case09(self):
        """创建dataset, storageConfigurations的值为空"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                "storageConfigurations": {}, "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case08', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storageConfigurations为空时err_code不正确')

    def test_case10(self):
        """创建dataset, storageConfigurations缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
                 "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storageConfigurations缺失时err_code不正确')

    def test_case11(self):
        """创建dataset, schema缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        data = {"name": dataset_name,
                "storage": "HDFS",
                "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 900, '创建dataset, schema缺失时err_code不正确')

    def test_case12(self):
        """--创建dataset, schema值为空--"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        data = {"name": dataset_name,
                "schema": {},
                "storage": "HDFS",
                "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        self.assertEqual(res.status_code, 500, 'Schema value 为空时status_code不正确')

    def test_case13(self):
        """创建dataset, storage非法"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": dataset_name,
                "schema": schema_info,
                "storage": "HDF",
                "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
        data = {"name": dataset_name,
                "schema": schema_info,
                "storage": "",
                "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, storage为空时err_code不正确')

    def test_case15(self):
        """创建dataset, storage缺失"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
        schema_info = schema()
        data = {"name": dataset_name,
                "schema": schema_info,
                "expiredPeriod": 0,
                "storageConfigurations": storageConfigurations,
                "sliceTime": "", "sliceType": "H",
                "owner": owner, "resource": dataset_resource}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, 'test_case09', res.text)
        err = json.loads(res.text)
        err = json.loads(err["err"])
        err_code = int(err["list"][0]["code"])
        self.assertEqual(err_code, 902, '创建dataset, schema缺失时err_code不正确')






# if __name__ == '__main__':
#     unittest.main()
