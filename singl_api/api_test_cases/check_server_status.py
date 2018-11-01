import requests
import unittest
from basic_info.get_auth_token import get_headers
from basic_info.setting import MY_LOGIN_INFO


# 该脚本用来查询系统服务状态
class Check_status(unittest.TestCase):
    """查询系统服务状态"""
    def test_case01(self):
        """查询系统服务状态"""
        url = '%s/api/component_status' % MY_LOGIN_INFO["HOST"]
        res = requests.get(url=url, headers=get_headers())
        # print(res.status_code, res.text)
        # 检查响应状态码是否200
        self.assertEqual(res.status_code, 200, '响应状态码不是200，服务异常')



