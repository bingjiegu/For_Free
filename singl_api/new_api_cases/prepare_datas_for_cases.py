# coding:utf-8
import os
import time

from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers, get_headers_upload
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.setting import HOST_189
# from new_api_cases.execute_cases import deal_parameters
from requests_toolbelt.multipart.encoder import MultipartEncoder
import random
from new_api_cases.get_statementId import statementId, statementId_no_dataset, get_sql_analyse_statement_id, get_sql_analyse_dataset_info, get_sql_execte_statement_id, steps_sql_parseinit_statemenId, steps_sql_analyzeinit_statementId


ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def get_job_tasks_id(job_id):
    url = '%s/api/woven/collectors/%s/tasks' % (HOST_189, job_id)
    data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
    response = requests.post(url=url, headers=get_headers(), json=data)
    tasks = dict_res(response.text)['content']
    all_task_id = []
    try:
        for item in tasks:
            task_id = item['id']
    except Exception as e:
        print(e)
        return
    else:
        all_task_id.append(task_id)
        # print(all_task_id)
        return all_task_id


def stop_job_task(job_id):
    url = '%s/api/woven/collectors/WOVEN-SERVER/stopTaskList' % HOST_189
    task_id = get_job_tasks_id(job_id)
    print(task_id)
    response = requests.post(url=url, headers=get_headers(), json=task_id)
    print(response.url)
    print(response.status_code, response.text)


def create_new_user(data):
    url = '%s/api/woven/users' % HOST_189
    response = requests.post(url=url, headers=get_headers(),json=data)
    user_id = dict_res(response.text)["id"]
    print(user_id)
    return user_id

def upload_files():
    url = 'http://192.168.1.189:8515/api/woven/upload/read/excel?maxSheet=1&maxRow=10000&maxColumn=3'
    # files = {'file': open(r'民族国家标准代码表.xls'', 'rb')}
    # file = 'E:\standbd\民族国家标准代码表.xls'
    # multipart_encoder = MultipartEncoder(
    #     fields={
    #         ('民族国家标准代码表.xls', open('E:\standbd\民族国家标准代码表.xls', 'rb'), 'application/octet-stream')
    #     },
    #     boundary='----------------'+str(random.randint(1e28, 1e29 - 1))
    #
    # )
    # headers = get_headers_upload()
    # headers['Content-Type'] = multipart_encoder.content_type

#     with open(r'E:\standbd\民族国家标准代码表.xls','rb') as f:
#         res = requests.post(url=url, headers=get_headers(), files=f)
#         print(res.status_code)
#         print(res.text)
# upload_files()


def collector_schema_sync(data):
    """获取采集器元数据同步后返回的task id"""
    collector_id = 'c1'
    # data = '{"useSystemStore": true, "dataSource":{"id": "f8523e1f-b1ff-48cd-be8d-02ab91290d5b", "name": "mysql_test_bj", "type": "JDBC", "driver": "com.mysql.jdbc.Driver", "url": "jdbc:mysql://192.168.1.189:3306/test", "username": "merce", "password": "merce", "dateToTimestamp":false, "catalog": "", "schema": "", "table": "", "selectSQL": "", "dbType": "DB"}, "dataStore":{"path": "/tmp/c1/mysql_test_bj", "format": "csv", "separator": ",", "type": "HDFS"}}'
    url = '%s/api/woven/collectors/%s/schema/fetch' % (HOST_189, collector_id)
    response = requests.post(url=url, headers=get_headers(), data=data)
    time.sleep(3)
    return response.text

def get_flow_id():
    name = "gbj_for_project_removeList" + str(random.randint(0,999999999999))
    data = {"name": name, "flowType": "dataflow",
            "projectEntity": {"id": "e47fe6f4-6086-49ed-81d1-68704aa82f2d"}, "steps": [], "links": []}
    url = '%s/api/flows/create' % HOST_189
    response = requests.post(url=url, headers=get_headers(), json=data)
    flow_id = dict_res(response.text)['id']
    print(flow_id)
    return flow_id


# get_flow_id()
