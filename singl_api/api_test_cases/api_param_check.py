from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO
from basic_info.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
# 创建flow的API路径
url = "%s/api/flows/create" % MY_LOGIN_INFO["HOST"]


# 该脚本用来校验create flow接口的参数
class For_Create_flow(unittest.TestCase):
    """校验create flow接口的参数"""

    def test_case01(self):
        """创建flow时， name为空"""
        # flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'streamflow'
        data = {"name": "", "flowType": "streamflow", "resource": {"id": "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"},
                "steps": [], "links": []}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
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

# if __name__ == '__main__':
#     unittest.main()