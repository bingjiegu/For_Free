from basic_info.get_auth_token import get_headers
# from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO,Flows_resourceid,idnameage_schema_name,idnameage_schema_id,tenant_id,\
    left_age_dataset_name,left_age_dataset_id,query_flow_name,query_flow_version,flow_update_id
from basic_info.Open_DB import MYSQL
from basic_info.url_info import query_flows_url,create_flows_url,query_flowname_url,query_flowname_version_url,query_flow_all_url,flow_update_url,\
    query_flow_history_id_url,query_flow_history_version_url,query_flow_flowAscheduler_id_url,query_flow_version_url


# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

# 该脚本用来测试编辑flow的场景
class API_flows(unittest.TestCase):
    """用来测试flow"""
    def test_case01(self):
        """正常查询流程分页-EQUAL"""

        data = {"fieldList":[{"fieldName":"parentId","fieldValue":Flows_resourceid,"comparatorOperator":"EQUAL"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        res = requests.post(url=query_flows_url, headers=get_headers(), data=json.dumps(data))
        # response
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询出的flow id, name, flowType，并组装成一个dict， 和response对比
        SQL = 'select id, name,flow_type from merce_flow ORDER BY last_modified_time DESC LIMIT 8'
        flow_query_info = ms.ExecuQuery(SQL)
        flow_query_name = flow_query_info[0][1]  # flow name
        flow_query_id = flow_query_info[0][0]  # flow id
        flow_query_flow_type = flow_query_info[0][2]  # flow type
        # print(response_text['content'][0]['flowType'])
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow查询后返回的status_code不正确')
        self.assertEqual(response_text['content'][0]['id'], flow_query_id, 'flow查询ID不相等' )
        self.assertEqual(response_text['content'][0]['name'], flow_query_name, 'flow查询name不相等')
        self.assertEqual(response_text['content'][0]['flowType'], flow_query_flow_type, 'flowflow_type不一致')
        time.sleep(5)
    def test_case02(self):
        """正常查询流程分页-LIKE"""

        data = {"fieldList":[{"fieldName":"parentId","fieldValue":Flows_resourceid,"comparatorOperator":"EQUAL"},{"fieldName":"name","fieldValue":"%test%","comparatorOperator":"LIKE"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        res = requests.post(url=query_flows_url, headers=get_headers(), data=json.dumps(data))
        # response
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询出flow id, name, flowType，并组装成一个dict， 和response对比
        SQL = 'select id, name,flow_type from merce_flow where name like "%test%" ORDER BY last_modified_time DESC LIMIT 8'
        flow_query_info = ms.ExecuQuery(SQL)
        flow_query_name = flow_query_info[0][1]  # flow name
        flow_query_id = flow_query_info[0][0]  # flow id
        flow_query_flow_type = flow_query_info[0][2]  # flow type
        # print(response_text['content'][0]['flowType'])
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow查询后返回的status_code不正确')
        self.assertEqual(response_text['content'][0]['id'], flow_query_id, 'flow查询ID不相等2')
        self.assertEqual(response_text['content'][0]['name'], flow_query_name, 'flow查询name不相等2')
        self.assertEqual(response_text['content'][0]['flowType'], flow_query_flow_type, 'flowflow_type不一致2')
        time.sleep(5)
    def test_case03(self):
        """正常创建流程-含有steps"""

        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'tc_auto_df_source_source'
        flow_sink_dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'tc_auto_df_source_sink_out'

        data = '{"name": "'+flow_name+'", "flowType": "dataflow", "resource": {"id": "'+Flows_resourceid+'"}, "steps": [{"id":"source_0","type":"source","x":140,"y":180,"name":"source_0","outputConfigurations":[{"id":"output","fields":[{"column":"id","alias":""},{"column":"name","alias":""},{"column":"age","alias":""}]}],"otherConfigurations":{"dataset":"'+left_age_dataset_name+'","datasetId":"'+left_age_dataset_id+'","schema":"'+idnameage_schema_name+'","schemaId":"'+idnameage_schema_id+'"}},{"id":"sink_0","type":"sink","x":366,"y":173,"name":"sink_0","inputConfigurations":[{"id":"input","fields":[{"column":"id"},{"column":"name"},{"column":"age"}]}],"outputConfigurations":null,"otherConfigurations":{"dataset":"'+flow_sink_dataset_name+'","schema":"'+idnameage_schema_name+'","schemaId":"'+idnameage_schema_id+'","type":"HDFS","format":"csv","separator":",","quoteChar":"\\"","escapeChar":"\\\\","path":"/tmp/py/out/source/auto/'+flow_sink_dataset_name+'","sql":"","table":"","specifiedStringColumnTypes":[{"name":"","dataType":"","length":""}],"driver":"","url":"","user":"","password":"","brokers":"","topic":"","groupId":"","partitionColumns":"","namespace":"","columns":"","description":"","expiredTime":"0","sliceTimeColumn":"","sliceType":"H","mode":"append","nullValue":""}}], "links": [{"source":"source_0","target":"sink_0","targetInput":"input"}],"tenant":{"id":"'+tenant_id+'"}}'

        res = requests.post(url=create_flows_url, headers=get_headers(), data=data)
        # response
        # print(res.status_code,res.text)
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_Type = flow_info[0][1]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow创建后返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等')
        self.assertEqual(response_text["flowType"], flow_Type, 'flow创建后flow_type不一致')
        time.sleep(3)
    def test_case04(self):
        """根据名称查询流程"""
        # 该接口没有返回值
        res = requests.get(url=query_flowname_url, headers=get_headers())

        # print(res.status_code,res.text)
        # response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        # SQL = 'select id, name from merce_flow where name = "%s"' % query_flow_name
        # flow_info = ms.ExecuQuery(SQL)
        # flow_id = flow_info[0][0]
        # flow_Name = flow_info[0][1]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 204, 'flow根据name查询返回的status_code不正确')
        # self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等')
        # self.assertEqual(response_text["name"], flow_Name, 'flow创建后name不一致')
        time.sleep(3)
    def test_case05(self):
        """根据名称和版本查询历史流程"""

        res = requests.get(url=query_flowname_version_url, headers=get_headers())
        # print(query_flowname_version_url)
        # print(res.status_code,res.text)
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where name= "%s"and version= "%s"' % (query_flow_name,query_flow_version)
        # print(SQL)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        self.assertEqual(res.status_code, 200, 'flow根据名称和版本查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow查询后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, 'flow查询后version不一致')
        time.sleep(3)

    def test_case06(self):
        """查询简化版流程"""
        # 断言只写了个200

        res = requests.get(url=query_flow_all_url, headers=get_headers())

        response_text = json.loads(res.text)

        self.assertEqual(res.status_code, 200, 'flow查询简化版流程查询返回的status_code不正确')
        time.sleep(3)
    # def test_case07(self):
    #     """更新流程"""
    #     # 还没写完
    #     res = requests.get(url=query_flow_all_url, headers=get_headers())
    #
    #     # print(res.status_code,res.text)
    #     response_text = json.loads(res.text)
    #     # print(response_text)
    #     # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
    #     # SQL = 'select id, name from merce_flow where name = "%s"' % query_flow_name
    #     # flow_info = ms.ExecuQuery(SQL)
    #     # flow_id = flow_info[0][0]
    #     # flow_Name = flow_info[0][1]
    #     # print(flow_id, flow_Type)
    #     # print(type(response_text), response_text)
    #     self.assertEqual(res.status_code, 200, '更新流程返回的status_code不正确')
    #     # self.assertEqual(response_text["id"], flow_id, 'flow查询简化版流程后查询ID不相等')
    #     # self.assertEqual(response_text["name"], flow_Name, 'flow查询简化版流程后name不一致')
    #     time.sleep(3)

    def test_case08(self):
        """根据老的版本查询历史流程"""

        res = requests.get(url=query_flow_history_version_url, headers=get_headers())

        # print(res.status_code,res.text)
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow id, version, 并组装成一个dict， 和response对比
        SQL = 'SELECT oid,version from merce_flow_history where name= "%s" and version<= "%s" ORDER BY version desc' % (query_flow_name,query_flow_version)
        flow_info = ms.ExecuQuery(SQL)
        flow_oid = flow_info[0][0]
        flow_version = flow_info[0][1]
        # print(flow_id, flow_Type)

        self.assertEqual(res.status_code, 200, 'flow根据老的版本查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_oid, 'flow根据老的版本查询历史流程后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, 'flow根据老的版本查询历史流程后version不一致')
        time.sleep(3)
    def test_case09(self):
        """根据老的id查询历史流程"""

        res = requests.get(url=query_flow_history_id_url, headers=get_headers())

        # print(res.status_code,res.text)
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow_id, flow_version, 并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where name= "%s" ORDER BY version desc' % (query_flow_name)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        # print(response_text[0]["id"])
        self.assertEqual(res.status_code, 200, 'flow根据老的id查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text[0]["id"], flow_id, 'flow根据老的id查询历史流程后查询ID不相等')
        self.assertEqual(response_text[0]["version"], flow_version, 'flow根据老的id查询历史流程后version不一致')
        time.sleep(3)

    # def test_case10(self):
    #     """根据流程id和计划id查询执行历史"""
    #     # 测试未通过
    #
    #     res = requests.get(url=query_flow_flowAscheduler_id_url, headers=get_headers())
    #
    #     print(res.status_code,res.text)
    #     # response_text = json.loads(res.text)
    #     # print(response_text)
    #     # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
    #     # SQL = 'SELECT id,version from merce_flow_history where name= "%s" and version<= "%s" ORDER BY version desc' % (query_flow_name,query_flow_version)
    #     # flow_info = ms.ExecuQuery(SQL)
    #     # flow_id = flow_info[0][0]
    #     # flow_version = flow_info[0][1]
    #     # print(flow_id, flow_Type)
    #     # print(type(response_text), response_text)
    #     # print(response_text[0]["id"])
    #     self.assertEqual(res.status_code, 200, '根据流程id和计划id查询执行历史查询返回的status_code不正确')
    #     # self.assertEqual(response_text[0]["id"], flow_id, 'flow根据老的id查询历史流程后查询ID不相等')
    #     # self.assertEqual(response_text[0]["version"], flow_version, 'flow根据老的id查询历史流程后version不一致')
    #     time.sleep(3)
    def test_case11(self):
        """根据老的版本查询流程"""


        res = requests.get(url=query_flow_version_url, headers=get_headers())

        # print(res.status_code,res.text)
        response_text = json.loads(res.text)
        # print(response_text)
        # 查询创建的flow flow_id, flow_version，并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where oid= "%s" and version= "%s"' % (flow_update_id,query_flow_version)
        # print(SQL)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        # print(flow_id, flow_Type)
        # print(type(response_text), response_text)
        # print(response_text[0]["id"])
        self.assertEqual(res.status_code, 200, '根据老的版本查询流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, '根据老的版本查询流程后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, '根据老的版本查询流程后version不一致')
        time.sleep(3)

    # def test_case12(self):
    #     """更新版本"""
    #     # 未完成
    #
    #     res = requests.get(url=query_flow_version_url, headers=get_headers())
    #
    #     print(res.status_code, res.text)
    #     response_text = json.loads(res.text)
    #     # print(response_text)
    #     # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
    #     SQL = 'SELECT id,version from merce_flow_history where oid= "%s" and version= "%s"' % (
    #     flow_update_id, query_flow_version)
    #     print(SQL)
    #     flow_info = ms.ExecuQuery(SQL)
    #     flow_id = flow_info[0][0]
    #     flow_version = flow_info[0][1]
    #     # print(flow_id, flow_Type)
    #     # print(type(response_text), response_text)
    #     # print(response_text[0]["id"])
    #     self.assertEqual(res.status_code, 200, '根据老的版本查询流程查询返回的status_code不正确')
    #     self.assertEqual(response_text["id"], flow_id, '根据老的版本查询流程后查询ID不相等')
    #     self.assertEqual(response_text["version"], flow_version, '根据老的版本查询流程后version不一致')
    #     time.sleep(3)


# if __name__ == '__main__':
#     unittest.main()

