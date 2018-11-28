from basic_info.get_auth_token import get_headers
import requests, json, time


# 根据flowid查询需要更新流程所需的flow_body
def get_flow_update_body():
    from basic_info.url_info import flow_update_flowid_url
    res = requests.get(url=flow_update_flowid_url, headers=get_headers())
    return json.loads(res.text)


# 创建flow用来删除

def create_flow():
    from basic_info.setting import MySQL_CONFIG
    from basic_info.Open_DB import MYSQL
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    from basic_info.setting import Flows_resourceid, flow_delete_name
    from basic_info.url_info import create_flows_url

    flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + flow_delete_name
    data = {"name": flow_name, "flowType": "dataflow",
            "resource": {"id": Flows_resourceid}, "steps": [], "links": []}
    res = requests.post(url=create_flows_url, data=json.dumps(data), headers=get_headers())
    response_text = json.loads(res.text)
    flow_delete_id = response_text["id"]

    return flow_delete_id
    time.sleep(3)


# 项目目录下创建flow用来删除
def create_flow_project():
    from basic_info.setting import MySQL_CONFIG
    from basic_info.Open_DB import MYSQL
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    from basic_info.setting import Flows_project_resourceId, flow_delete_name
    from basic_info.url_info import create_flows_url

    flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + flow_delete_name
    data = {"name": flow_name, "flowType": "dataflow",
            "projectEntity": {"id": Flows_project_resourceId}, "steps": [], "links": []}
    res = requests.post(url=create_flows_url, data=json.dumps(data), headers=get_headers())
    response_text = json.loads(res.text)
    flow_project_delete_id = response_text["id"]
    return flow_project_delete_id
    time.sleep(3)
