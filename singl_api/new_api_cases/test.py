import requests
from requests_toolbelt import MultipartEncoder

from basic_info.get_auth_token import get_headers

def upload_excel_file():
    url = "http://192.168.1.189:8515/api/woven/upload/read/excel"
    files = {'file': open('E:\standbd\性别分类.xls', 'rb')}
    headers = get_headers()
    headers.pop('Content-Type')
    print(headers)
    querystring = {"maxSheet": "1", "maxRow": "10000", "maxColumn": "3"}
    response = requests.post(url, files=files, headers=headers, params=querystring)
    print(response.status_code, response.text)

def upload_jar_file():
    url = "http://192.168.1.57:8515/api/processconfigs/uploadjar/filter%20class"
    files = {"file": open('E:\woven-common-3.0.jar', 'rb')}
    headers = get_headers()
    headers.pop('Content-Type')
    response = requests.post(url, files=files, headers=headers)
    print(response.text)

# upload_excel_file()
upload_jar_file()
