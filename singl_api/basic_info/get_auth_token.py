# coding:utf-8
import requests
from basic_info.setting import MY_LOGIN_INFO2


# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token():
    res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"], data=MY_LOGIN_INFO2["DATA"])
    # print(res.headers)
    # x_token = res.headers['X-AUTH-TOKEN']
    # print(x_auth_token)
    return res.headers['X-AUTH-TOKEN']



# 组装headers， 接口请求时调用
def get_headers():
    x_auth_token = get_auth_token()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    return headers






