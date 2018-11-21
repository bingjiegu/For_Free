import requests
from basic_info.setting import MY_LOGIN_INFO


# 获取登录后返回的X-AUTH-TOKEN
def get_auth_token():
    res = requests.post(url=MY_LOGIN_INFO["URL"], headers=MY_LOGIN_INFO["HEADERS"], data=MY_LOGIN_INFO["DATA"])
    x_auth_token = res.headers['X-AUTH-TOKEN']
    # print(x_auth_token)
    return x_auth_token


# 组装headers， 接口请求时调用
def get_headers():
    x_auth_token = get_auth_token()
    headers = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token, "Accept": "application/json"}
    return headers


# 组装headers， 导出流程接口请求时调用
def get_headers_flow():
    x_auth_token = get_auth_token()
    headers1 = {'Content-Type': 'application/json', "X-AUTH-TOKEN": x_auth_token,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}
    return headers1
