import requests
from basic_info.setting import HOST_189
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

def upload_excel_file():
    url = "%s/api/woven/upload/read/excel" % HOST_189
    files = {'file': open('性别分类.xls', 'rb')}
    headers = get_headers()
    headers.pop('Content-Type')
    print(headers)
    querystring = {"maxSheet": "1", "maxRow": "10000", "maxColumn": "3"}
    response = requests.post(url, files=files, headers=headers, params=querystring)
    print(response.status_code, response.text)

# upload_excel_file()


user_search = 'select id from merce_user where name = "dudu666666"'
user_search_result = ms.ExecuQuery(user_search)
print(type(user_search_result))
if user_search_result:
    user_id = user_search_result[0]["id"]
    print(user_id)
else:
    print('no result')