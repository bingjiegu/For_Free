from basic_info.get_auth_token import get_headers, get_auth_token
from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, owner, dataset_resource, schema_resource, MY_LOGIN_INFO, tenant_id
from basic_info.Open_DB import MYSQL



# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


# 该脚本用来测试创建flow的场景
class CreateFlow(unittest.TestCase):
    """用来测试创建flow"""
    from basic_info.url_info import create_flow_url

    def test_case01(self):
        """正常创建flow-dataflow"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'flow'
        data = {"name": flow_name, "flowType": "dataflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"}, "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        response_text = json.loads(res.text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0]["id"]
        flow_Type = flow_info[0]["flow_type"]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow创建后返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等' )
        self.assertEqual(response_text["flowType"], flow_Type, 'flow创建后flow_type不一致')
        time.sleep(5)

    def test_case02(self):
        """正常创建flow-workflow"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'workflow'
        data = {"name": flow_name, "flowType": "workflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"}, "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        response_text = json.loads(res.text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0]["id"]
        flow_Type = flow_info[0]["flow_type"]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow创建后返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等' )
        self.assertEqual(response_text["flowType"], flow_Type, 'flow创建后flow_type不一致')
        time.sleep(5)

    def test_case03(self):
        """正常创建flow-streamflow"""
        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": flow_name, "flowType": "streamflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"}, "steps": [], "links": []}
        res = requests.post(url=self.create_flow_url, headers=get_headers(), data=json.dumps(data))
        # response
        response_text = json.loads(res.text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0]["id"]
        flow_Type = flow_info[0]["flow_type"]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow创建后返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等')
        self.assertEqual(response_text["flowType"], flow_Type, 'flow创建后flow_type不一致')
        time.sleep(3)


class CreateDataSet(unittest.TestCase):
    """该脚本用来测试创建dataset的场景,包含正向流和异向流"""
    from basic_info.url_info import create_dataset_url
    storage = get_datasource()
    storageConfigurations = {"format": "csv", "path": "/tmp/gubingjie", "relativePath": "/tmp/gubingjie",
                             "recursive": "false", "header": "false", "separator": ",", "quoteChar": "\"",
                             "escapeChar": "\\"}

    dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'

    def test_case01(self):
        """--正常创建DBdataset，选择已存在的schema属性为true--"""
        try:
            schema_query = 'select id from merce_schema where name = "gbj_schema"'
            schema = ms.ExecuQuery(schema_query)
        except Exception as e:
            raise e
        else:
            schema_id = {}
            schema_id["id"] = schema[0]["id"]
            # print(schema_id)
            data = {"name": self.dataset_name, "expiredPeriod": 0, "storage": "JDBC", "storageConfigurations": self.storage, "schema": schema_id, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
            res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))

            # print(res.status_code, res.text)
            self.assertEqual(res.status_code, 201, 'DB-dataset创建失败')
            # time.sleep(2)

    def test_case02(self):
        """--正常创建HDFSdataset--"""
        dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'HDFSdataset'
        schema_info = schema()  # data_from_db中schema()查询schema
        data = {"name": dataset_name, "schema": schema_info, "storage": "HDFS", "expiredPeriod": 0,
        "storageConfigurations": self.storageConfigurations, "owner": "2059750c-a300-4b64-84a6-e8b086dbfd42", "resource": {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}}
        res = requests.post(url=self.create_dataset_url, headers=get_headers(), data=json.dumps(data))
        # 断言成功时响应状态码为201
        print("开始创建hdfs")
        print(res.status_code, res.text)
        self.assertEqual(res.status_code, 201, 'HDFS dataset创建失败')
        text = json.loads(res.text)
        text = text["id"]
        try:
            query = 'select id from merce_dataset where name = "%s"' % dataset_name
            new_dataset = ms.ExecuQuery(query)
        except:
            print('没有查询到datasetname为%s的dataset' % dataset_name)
        else:
            new_dataset = new_dataset[0]["id"]
            # print(text, new_dataset)
            # 根据datasetname查询到dataset ID， 并和返回的text中包含的ID进行对比
            self.assertEqual(text, new_dataset, '返回的dataset id和使用该dataset的 name查询出的id不相等')

class Get_DataSet(unittest.TestCase):
    """测试dataset查询接口"""

    def test_case01(self):
        """使用id查询"""
        try:
            dataset_sql = 'select id, name from merce_dataset order by create_time desc limit 1'
            dataset_info = ms.ExecuQuery(dataset_sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            # print(type(dataset_id[0][0]))
        except Exception as e:
            raise e
        else:
            url2 = '%s/api/datasets/%s?tenant=%s' % (MY_LOGIN_INFO["HOST"], dataset_id, tenant_id)
            response = requests.get(url=url2, headers=get_headers()).text
            response = json.loads(response)
            response_id = response["id"]
            response_name = response["name"]
            # print("id:", response["id"])
            # print({"id": dataset_id, "name": dataset_name} == {"id": response_id, "name": response_name})
            self.assertEqual({"id": dataset_id, "name": dataset_name}, {"id": response_id, "name": response_name}, '两次查询得到的dataset id和name不一致，查询失败')




