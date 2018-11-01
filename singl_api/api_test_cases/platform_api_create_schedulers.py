from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG,MY_LOGIN_INFO
from basic_info.Open_DB import MYSQL

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
url = "%s/api/schedulers" % MY_LOGIN_INFO["HOST"]

class Create_schedulers(unittest.TestCase):
    """用来测试创建schedulers"""
    # 创建schedulers的API路径
    def test_case01(self):
        """创建schedulers，单次执行"""
        scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schedulers'
        data = {"name": scheduler_name,
                "flowId": "1f028f3c-fd76-4e89-afa9-9c1d12b14946",
                "flowName": "gbj_dataflow",
                "flowType": "dataflow",
                "schedulerId": "once",
                "configurations":
                    {"startTime": int((time.time() + 7200)*1000), "arguments": [], "cron": "once", "properties": []}
                }
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        print(res.status_code, res.text)
        self.assertEqual(res.status_code, 201, '创建单次执行的scheduler失败')

    def test_case02(self):
        """创建schedulers，周期执行"""
        scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'scheduler'
        data = {"name": scheduler_name,
                "flowId": "1f028f3c-fd76-4e89-afa9-9c1d12b14946",
                "flowName": "gbj_dataflow",
                "flowType": "dataflow",
                "schedulerId": "cron",
                "source": "rhinos",
                "configurations":
                    {"arguments": [],
                     "cron": "0 0 12 * * ? ",
                     "cronType": "simple",
                     "endTime": 3153600000000,
                     "properties":
                         [{"name":"all.debug","value":"false"},
                          {"name":"all.dataset-nullable","value":"false"},
                          {"name":"all.notify-output","value":"false"},
                          {"name":"all.debug-rows","value":"20"},
                          {"name":"dataflow.master","value":"yarn"},
                          {"name":"dataflow.queue","value":["default"]},
                          {"name":"dataflow.num-executors","value":"2"},
                          {"name":"dataflow.driver-memory","value":"512M"},
                          {"name":"dataflow.executor-memory","value":"1G"},
                          {"name":"dataflow.executor-cores","value":"2"},
                          {"name":"dataflow.verbose","value":"true"},
                          {"name":"dataflow.local-dirs","value":""},
                          {"name":"dataflow.sink.concat-files","value":"true"}],
                     "startTime": int((time.time() + 7200)*1000)}}
        res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
        # print(res.status_code, res.text)
        self.assertEqual(res.status_code, 201, '创建周期执行的scheduler失败')
