import os

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
DATA_PATH = os.path.join(BASE_PATH,'test_cases')
REPORT_PATH = os.path.join(BASE_PATH,'report')
email_user = 'ruifan_test@163.com'  # 发送者账号
email_pwd = 'ruifantest'       # 发送者密码
email_list = {
    'gubingjie': "bingjie.gu@inforefiner.com"
}
email_to = {
    "gubingjie": "bingjie.gu@inforefiner.com",
    "daming": "zhiming.wang@inforefiner.com",
    "pengyuan":"yuan.peng@inforefiner.com"

}
MySQL_CONFIG = {
    'HOST': '192.168.1.189',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce',
    'case_db': 'test'
}
schema_id = "c71b8d28-6c5b-4b9f-a470-61eda073bd6e"
owner = "2059750c-a300-4b64-84a6-e8b086dbfd42"
tenant_id = "2d7ad891-41c5-4fba-9ff2-03aef3c729e5"  # default租戶ID
dataset_resource = {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}
schema_resource = {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}

MY_LOGIN_INFO = {
 "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
 "URL": "http://192.168.1.189:8515/api/auth/login",
 "DATA": {'name': 'admin', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'},
 "HOST": "http://192.168.1.189:8515"
}