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
dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'dataset'
storageConfigurations = {"format": "csv", "path": "/tmp/gubingjie", "relativePath": "/tmp/gubingjie",
                             "recursive": "false", "header": "false", "separator": ",", "quoteChar": "\"",
                             "escapeChar": "\\"}
url = '%s/api/datasets' % MY_LOGIN_INFO["HOST"]

storage = get_datasource()


class Get_DataSet(unittest.TestCase):
    """该脚本用来测试dataset查询接口"""

    def test_case01(self):
        """使用id查询"""
        try:
            name = "sink"
            dataset_sql = 'select id, name from merce_dataset where  name like "%%%s%%" order by create_time desc limit 1' % name
            dataset_info = ms.ExecuQuery(dataset_sql)
            dataset_id = dataset_info[0][0]
            dataset_name = dataset_info[0][1]
            print(type(dataset_id[0][0]))
        except Exception as e:
            raise e
        else:
            url2 = '%s/api/datasets/%s?tenant=%s' % (MY_LOGIN_INFO["HOST"], dataset_id, tenant_id)
            response = requests.get(url=url2, headers=get_headers()).text
            print(type(response))
            response = json.loads(response)
            print(type(response))
            print(response)
            response_id = response["id"]
            response_name = response["name"]

            # print("id:", response["id"])
            # print({"id": dataset_id, "name": dataset_name} == {"id": response_id, "name": response_name})
            # self.assertEqual({"id": dataset_id, "name": dataset_name}, {"id": response_id, "name": response_name}, '两次查询得到的dataset id和name不一致，查询失败')
