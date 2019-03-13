# coding:utf-8
import unittest
from basic_info.setting import MY_LOGIN_INFO2
import requests
import json


class CheckLogin(unittest.TestCase):
    """login 接口测试"""
    """测试登录接口"""
    def setUp(self):
        self.login_info = MY_LOGIN_INFO2["DATA"]
        self.login_header = MY_LOGIN_INFO2["HEADERS"]

    def test_case01(self):
        """正常登录"""
        from basic_info.url_info import login_url
        res = requests.post(url=login_url, headers=self.login_header, data=self.login_info)
        # print(res.status_code, res.text, res.json(), type(res.json()))
        self.assertEqual(res.status_code, 200)
        # assert res.status_code == 200

    def test_case02(self):
        """用户名错误"""
        from basic_info.url_info import login_url
        data = {'name': 'gbj_use88', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

    def test_case03(self):
        """密码错误"""
        from basic_info.url_info import login_url
        data = {'name': 'admin', 'password': '12345678', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

    def test_case04(self):
        """租户错误"""
        from basic_info.url_info import login_url
        data = {'name': 'admin', 'password': '12345678', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'defaul'}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

    def test_case05(self):
        """用户名为空"""
        from basic_info.url_info import login_url
        data = {'name': '', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

    def test_case06(self):
        """密码为空"""
        from basic_info.url_info import login_url
        data = {'name': 'admin', 'password': '', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

    def test_case07(self):
        """租户为空"""
        from basic_info.url_info import login_url
        data = {'name': 'admin', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': ''}
        res = requests.post(url=login_url, headers=self.login_header, json=data)
        # print(res.status_code, res.text)
        assert res.status_code == 400
        assert json.loads(res.text) == {"err": "the tenant null can not found"}

