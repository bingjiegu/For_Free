# coding:utf-8
import requests,json
from basic_info.setting import MY_LOGIN_INFO2, MY_LOGIN_INFO_root

# admin账户登录，普通请求
# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token():
    res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"], data=MY_LOGIN_INFO2["DATA"])
    dict_headers = dict(res.headers)
    token = dict_headers['X-AUTH-TOKEN']
    # print(token)
    return token


# 组装headers， 接口请求时调用
def get_headers():
    x_auth_token = get_auth_token()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    # print(headers)
    return headers


# admin账户登录，POST请求中上传file文件，需要使用不同的content-type
# 组装headers， 接口请求时调用
def get_headers_upload():
    x_auth_token = get_auth_token()
    headers = {"X-AUTH-TOKEN": x_auth_token,
               'Origin': 'http://192.168.1.189:8515',
               'Referer': 'http://192.168.1.189:8515/',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
               }
    # print(headers)
    return headers






# root用户登录使用
def get_auth_token_root():
    res = requests.post(url=MY_LOGIN_INFO_root["URL"], headers=MY_LOGIN_INFO_root["HEADERS"], data=MY_LOGIN_INFO_root["DATA"])
    dict_headers = dict(res.headers)
    token = dict_headers['X-AUTH-TOKEN']
    # print(token)
    return token


# 组装headers， 接口请求时调用
def get_headers_root():
    x_auth_token = get_auth_token_root()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    # print(headers)
    return headers
