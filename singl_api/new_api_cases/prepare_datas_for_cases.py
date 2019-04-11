# coding:utf-8
import os
from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.setting import HOST_189
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

