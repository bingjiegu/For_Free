import pymysql
import json
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, schema_id, scheduler_name,flow_id
import traceback
from basic_info.url_info import *
# from basic_info import url_info
from basic_info.get_auth_token import get_headers

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],)

# 时间戳长度转化函数，从10位浮点数转化为13位
def timestamp_to_13(time_stamp, digits=13):
    time_stamp = time_stamp.strftime('%a %b %d %H:%M:%S %Y')
    time_stamp = time.mktime(time.strptime(time_stamp))
    digits = 10 ** (digits - 10)
    time_stamp = int(round(time_stamp*digits))
    return time_stamp


# 获取schema基本信息和tenant_id, 作为参数传递给get_tenant()
def schema():
    try:
        # 根据setting文件中写入的schema id查询
        sql = 'select * from merce_schema where id = "%s"' % schema_id
        data = ms.ExecuQuery(sql)  # 执行SQL语句
    except:
        return None

    # 将浮点数的时间戳转化成13位格式后存入schema字典
    # lastModifiedTime = data[0][6]
    # create_time = ms[0][1]
    # create_time = timestamp_to_13(create_time, 13)
    # lastModifiedTime = timestamp_to_13(lastModifiedTime, 13)
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
    scheduler_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'schedulers_delete'
    flow_name = get_flows()[0][0]
    flow_type = get_flows()[0][1]
    # url = "%s/api/schedulers" % MY_LOGIN_INFO["HOST"]
    url = create_scheduler_url
    data = {"name": scheduler_name,
            "flowId": flow_id,
            "flowName": flow_name,
            "flowType": flow_type,
            "schedulerId": "once",
            "configurations":
                {"startTime": int((time.time() + 7200) * 1000), "arguments": [], "cron": "once", "properties": []}            }

    res = requests.post(url=url, headers=get_headers(), data=json.dumps(data))
    # print('get_n_schedulers执行return')
    return res.text





