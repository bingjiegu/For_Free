from basic_info.get_auth_token import get_headers
import unittest
import requests
import json
import time
from basic_info.format_res import dict_res, get_time
from basic_info.setting import MySQL_CONFIG, scheduler_id, flow_id
from basic_info.Open_DB import MYSQL
from basic_info.url_info import *


# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class query_execution(unittest.TestCase):
    """测试execution接口"""

    def test_case01(self):
        """1.execution查询：根据flow ID 查询execution"""
        data = {"fieldList":
                    [{"fieldName": "flowId", "fieldValue": flow_id, "comparatorOperator": "EQUAL"}],
                "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}}
        res = requests.post(url=query_exectution_url, headers=get_headers(), json=data)
        print(res.status_code, res.text)
        executions = dict_res(res.text)
        executions_flowId = executions["content"][0]["flowId"]
        print(len(executions["content"]))
        self.assertEqual(res.status_code, 200, "execution查询接口调用失败")
        self.assertEqual(flow_id, executions_flowId, "查询得到的execution中的flowId和查询使用的flowId不一致")

    # def test_case02(self):
    #     """2.execution查询：比对接口查询和数据库表查询得到的结果数是否一致"""
    #     data = {"fieldList": [{"fieldName": "flowId", "fieldValue": flow_id, "comparatorOperator": "EQUAL"}],
    #             "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}}
    #     res = requests.post(url=query_exectution_url, headers=get_headers(), json=data)
    #     # print(res.status_code, res.text)
    #     executions = dict_res(res.text)
    #     # 接口查询返回的execution个数
    #     executions_count1 = len(executions["content"])
    #     # 通过该flowid查询数据库中的execution个数
    #     execution_sql = 'select count(*) from merce_flow_execution where flow_id = "%s"' % flow_id
    #     executions_count2 = ms.ExecuQuery(execution_sql)
    #     executions_count2 = executions_count2[0][0]
    #     self.assertEqual(executions_count1, executions_count2, "接口查询和数据库表查询得到的execution个数不一致")
