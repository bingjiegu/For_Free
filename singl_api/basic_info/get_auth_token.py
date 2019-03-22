# coding:utf-8
import requests,json
from basic_info.setting import MY_LOGIN_INFO2


# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token():
    print('-----',MY_LOGIN_INFO2)
    res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"], data=MY_LOGIN_INFO2["DATA"])

    print('=====', res)
    print('1:', type(res.headers))
    dict_headers = dict(res.headers)
    print('2:', type(dict_headers))
    print('2-1:', dict_headers)
    token = dict_headers['X-AUTH-TOKEN']
    print('3:', token)
    print('4:', type(token))
    return token


# 组装headers， 接口请求时调用
def get_headers():
    x_auth_token = get_auth_token()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    print(headers)
    return headers


get_auth_token()
get_headers()


