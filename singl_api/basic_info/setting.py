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

# 查询scheduler时使用的name
scheduler_name = "gbj_dataflow 2018-10-17 16:23:19"

# 查询schedulers时使用的id（和"gbj_dataflow 2018-10-17 16:23:19"是同一个）
scheduler_id = "03c9d141-4374-4f24-a427-1781bfbfed58"

# 查询flow时使用的id
flow_id = "1f028f3c-fd76-4e89-afa9-9c1d12b14946"
# 查询flow时使用的resource的id
Flows_resourceid = "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"
# 创建flow时使用的schema的name和id
idnameage_schema_name = 'idnameage'
idnameage_schema_id = '0a80565f-10ef-4bea-8563-0cb28cd0db27'
left_age_dataset_name = 'left_age'
left_age_dataset_id = '8bfcf577-ebb6-4f8d-ae43-eac671ad5364'
# 根据flowname查询流程使用的name,version
query_flow_name = 'test_df_supplement'
query_flow_version = 3
# 更新流程时使用的流程id
flow_update_id = 'cb0a37ea-de4a-495c-bae0-236fcbd08eaf'
# 根据流程id和计划id查询执行历史
flow_scheduler_id = '63b0a864-ce40-4f88-a25d-929164198087'

MY_LOGIN_INFO = {
 "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
 "URL": "http://192.168.1.189:8515/api/auth/login",
 "DATA": {'name': 'admin', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'},
 "HOST": "http://192.168.1.189:8515"
}