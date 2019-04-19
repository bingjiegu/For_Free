# coding:utf-8
import os
import time
from urllib import parse

from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers, get_headers_upload
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.setting import HOST_189
from selenium import webdriver
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


# def stop_job_task(job_id):
#     url = '%s/api/woven/collectors/WOVEN-SERVER/stopTaskList' % HOST_189
#     task_id = get_job_tasks_id(job_id)
#     print(task_id)
#     response = requests.post(url=url, headers=get_headers(), json=task_id)
#     print(response.url)
#     print(response.status_code, response.text)


def create_new_user(data):
    url = '%s/api/woven/users' % HOST_189
    response = requests.post(url=url, headers=get_headers(),json=data)
    user_id = dict_res(response.text)["id"]
    print(user_id)
    return user_id

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

def get_applicationId():
    """进入yarn页面，获取状态为finished的application id"""
    # 进入yarn页面，获取状态为finished的application id
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.implicitly_wait(10)
    # 进入ambari页面，然后进入yarn页面
    driver.get('http://192.168.1.81:8080/#/main/services/YARN/heatmaps')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[1]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[2]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/button').click()
    driver.get('http://info2:8088/cluster')
    driver.get('http://info2:8088/cluster/apps/FINISHED')
    # 获取所有finished状态的application id
    all_applications = driver.find_elements_by_xpath('.//*[@id="apps"]/tbody/tr/td[1]/a')
    # 返回第一个application id，提供给case进行查询该applicationId的log
    application_id = all_applications[0].text
    time.sleep(3)
    # print(application_id)
    # print(type(application_id))
    return application_id


def upload_files():
    url = 'http://192.168.1.189:8515/api/woven/upload/read/excel?maxSheet=1&maxRow=10000&maxColumn=3'
    headers = get_headers()
    headers['Content-Type'] = 'multipart/form-data; boundary=----WebKitFormBoundarydCTDwRoe6Ox1gnPn'
    headers['Cookie'] = 'userName=admin; userPwd=123456; AMBARISESSIONID=1rmk20wkx3urr1hjkp6ujf5rqn'
    with open(r'E:\standbd\性别分类.xls', 'rb') as f:
        res = requests.post(url=url, headers=headers, files=f)
        print(res.status_code)
        print(res.text)


def get_woven_qaoutput_dataset_path():
    """查找woven/qaoutput下的所有数据集name，并组装成woven/qaoutput/datasetname的格式"""
    url = '%s/api/datasets/query' % HOST_189
    data = {"fieldList":[{"fieldName":"parentId","fieldValue":"4f4d687c-12b3-4e09-9ba9-bcf881249ea0","comparatorOperator":"EQUAL","logicalOperator":"AND"},{"fieldName":"owner","fieldValue":"2059750c-a300-4b64-84a6-e8b086dbfd42","comparatorOperator":"EQUAL","logicalOperator":"AND"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
    response = requests.post(url=url,headers=get_headers(), json=data)
    contents = dict_res(response.text)["content"]
    path = []
    for content in contents:
        content_paths = 'woven/qaoutput/' + content["name"]
        print(content_paths)
        content_path = b'%s' % content_paths
        print(content_path, type(content_path))
    #     new_content_path = parse.quote(parse.quote('%s' % content_path, safe=b''))
    #     print(new_content_path)
    #     path.append(new_content_path)
    # # print(path)
    # return path

# get_woven_qaoutput_dataset_path()