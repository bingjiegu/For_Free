
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO

from basic_info.get_auth_token import get_headers
from basic_info.data_from_db import get_flows, flow_id
import unittest
import json


# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


def create_schedulers():
    from basic_info.url_info import create_scheduler_url

    scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schedulers_delete'
    flow_name = get_flows()[0][0]
    flow_type = get_flows()[0][1]
    data = {"name": scheduler_name,
            "flowId": flow_id,
            "flowName": flow_name,
            "flowType": flow_type,
            "schedulerId": "once",
            "configurations":
                {"startTime": int((time.time() + 7200) * 1000), "arguments": [], "cron": "once", "properties": []}}

    res = requests.post(url=create_scheduler_url, headers=get_headers(), data=json.dumps(data))
    return res.text







