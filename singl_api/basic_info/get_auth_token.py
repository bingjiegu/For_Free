# coding:utf-8
import requests,json
from basic_info.setting import MY_LOGIN_INFO2


# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token():
    res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"], data=MY_LOGIN_INFO2["DATA"])
    dict_headers = dict(res.headers)
    token = dict_headers['X-AUTH-TOKEN']
    return token


# 组装headers， 接口请求时调用
def get_headers():
    x_auth_token = get_auth_token()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    print(headers)
    return headers




