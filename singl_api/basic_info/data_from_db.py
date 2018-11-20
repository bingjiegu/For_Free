import pymysql
import json
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, schema_id, scheduler_name,flow_id, MY_LOGIN_INFO
import traceback
from basic_info.get_auth_token import get_headers

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],)


# 获取schema基本信息和tenant_id, 作为参数传递给get_tenant()
def schema():
    try:
        # 根据setting文件中写入的schema id查询
        sql = 'select * from merce_schema where id = "%s"' % schema_id
        data = ms.ExecuQuery(sql)  # 执行SQL语句
    except:
        return None
    else:
        # 使用字典存储返回的schema id 和 name
        schema = {}
        schema['id'] = data[0][0]
        schema['name'] = data[0][9]
        return schema


# 根据DataSource ID查询DataSource的基本信息
def get_datasource():
    """storageConfigurations"""

    # 根据id查询DataSource的信息
    try:
        datasource_sql = 'SELECT * from merce_dss WHERE id = "a1ef7bdf-9120-4470-9962-11e01a518bc4"'
        datasource_info = ms.ExecuQuery(datasource_sql)
    except:
        return None
    # print(datasource_info)
    # print(type(datasource_info))
    else:
        # 用字典来存储DataSource的基本信息，可以在创建dataset时直接调用
        storageConfigurations = {}
        storageConfigurations['name'] = datasource_info[0][9]  # datasource name
        storageConfigurations['id'] = datasource_info[0][0]  # datasource id
        storageConfigurations['resType'] = "DB"
        storageConfigurations['table'] = 'city'   # 指定table
        DB_info = json.loads(datasource_info[0][13])  # datasource DB info
        print(DB_info)
        for k, v in DB_info.items():  # 将DB信息存入storageConfigurations
            if k != 'properties':
              storageConfigurations[k] = v
        return storageConfigurations


def get_schedulers():
    try:
        sql = 'select id, name from merce_flow_schedule where name = "%s"' % scheduler_name
        scheduler_id = ms.ExecuQuery(sql)
    except Exception as e:
        print("scheduler数据查询出错:%s" %e)
    else:
        scheduler_id = scheduler_id[0][0]
        return scheduler_id


def get_flows():
    try:
        sql = 'select name, flow_type from merce_flow where id = "%s"' % flow_id
        flow_info = ms.ExecuQuery(sql)

    except Exception as e:
        traceback.print_exc()
    else:
        return flow_info


def create_schedulers():
    from basic_info.url_info import create_scheduler_url
    scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schedulers_delete'
    flow_name = get_flows()[0][0]
    flow_type = get_flows()[0][1]
    data = {"name": scheduler_name,
            "flowId": flow_id,
            "flowName": flow_name,
            "flowType": flow_type,
            "schedulerId": "once",
            "configurations":
                {"startTime": int((time.time() + 7200) * 1000), "arguments": [], "cron": "once", "properties": []}}

    res = requests.post(url=create_scheduler_url, headers=get_headers(), data=json.dumps(data))
    return res.text

def NewFlow():
    from basic_info.url_info import create_flows_url




