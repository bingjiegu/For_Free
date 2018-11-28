import pymysql
import json
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, schema_id, scheduler_name,flow_id, MY_LOGIN_INFO
import traceback
from basic_info.get_auth_token import get_headers
from basic_info.format_res import get_time, dict_res
import random

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
    flow_name = get_flows()[0]["name"]
    flow_type = get_flows()[0]["flow_type"]
    data = {
    "configurations":{
        "arguments":[],
        "properties":[
            {
                "name":"all.debug",
                "value":"false"
            },
            {
                "name":"all.dataset-nullable",
                "value":"false"
            },
            {
                "name":"all.lineage.enable",
                "value":"true"
            },
            {
                "name":"all.notify-output",
                "value":"false"
            },
            {
                "name":"all.debug-rows",
                "value":"20"
            },
            {
                "name":"dataflow.master",
                "value":"yarn"
            },
            {
                "name":"dataflow.deploy-mode",
                "value":"client"
            },
            {
                "name":"dataflow.queue",
                "value":"a1"
            },
            {
                "name":"dataflow.num-executors",
                "value":"2"
            },
            {
                "name":"dataflow.driver-memory",
                "value":"512M"
            },
            {
                "name":"dataflow.executor-memory",
                "value":"1G"
            },
            {
                "name":"dataflow.executor-cores",
                "value":"2"
            },
            {
                "name":"dataflow.verbose",
                "value":"true"
            },
            {
                "name":"dataflow.local-dirs",
                "value":""
            },
            {
                "name":"dataflow.sink.concat-files",
                "value":"true"
            }
        ],
        "startTime": get_time()
    },
    "flowId":flow_id,
    "flowName": flow_name,
    "flowType": flow_type,
    "name":"students_flow" + str(random.randint(0, 99999)),
    "schedulerId":"once",
    "source":"rhinos"
}
    res = requests.post(url=create_scheduler_url, headers=get_headers(), data=json.dumps(data))
    # print(res.status_code, res.text)
    if res.status_code == 201 and res.text:
        scheduler_id_format = dict_res(res.text)
        try:
            scheduler_id = scheduler_id_format["id"]
        except KeyError as e:
            print("scheduler_id_format中存在异常%s" % e)
        else:
            return scheduler_id
    else:
        return None


def get_e_finial_status(scheduler_id):
    if scheduler_id:
        # print("查询前先等待10S")
        # time.sleep(10)
        execution_sql = 'select id, status, flow_id , flow_scheduler_id from merce_flow_execution where flow_scheduler_id = "%s" ' % scheduler_id
        select_result = ms.ExecuQuery(execution_sql)
        # print("根据scheduler id %s 查询execution，查询结果 %s: " % (scheduler_id, select_result))
        if select_result:
            e_info = {}
            # 从查询结果中取值
            try:
                e_id = select_result[0]["id"]
                print(e_id)
                e_info["e_id"] = e_id
                e_info["flow_id"] = select_result[0]["flow_id"]
                e_info["flow_scheduler_id"] = select_result[0]["flow_scheduler_id"]
                e_status = select_result[0]["status"]
            except IndexError as e:
                print("取值时报错 %s" % e)
                raise e
            else:
                # 对返回数据格式化
                e_status_format = dict_res(e_status)
                e_final_status = e_status_format["type"]
            e_info["e_final_status"] = e_final_status  #
            # 将 execution id , flow_id和status组装成字典的形式并返回
            return e_info
        else:
            # print("根据scheduler id: %s ,没有查找到execution" % scheduler_id)
            return None
    else:
        return None

if __name__ == '__main__':
    print(get_e_finial_status("cb3c3f67-1087-4a39-859e-f79021d30654"))

