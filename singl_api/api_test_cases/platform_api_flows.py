from basic_info.get_auth_token import get_headers
# from basic_info.data_from_db import get_datasource, schema
import unittest
import requests
import json
import time
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO, Flows_resourceid, idnameage_schema_name, \
    idnameage_schema_id, tenant_id, \
    left_age_dataset_name, left_age_dataset_id, query_flow_name, query_flow_version, flow_update_id
from basic_info.Open_DB import MYSQL
from basic_info.url_info import *

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


# 该脚本用来测试编辑flow的场景
class API_flows(unittest.TestCase):
    """用来测试flow"""

    def test_case01(self):
        """正常查询流程分页-EQUAL"""

        data = {"fieldList": [{"fieldName": "parentId", "fieldValue": Flows_resourceid, "comparatorOperator": "EQUAL"}],
                "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
        res = requests.post(url=query_flows_url, headers=get_headers(), data=json.dumps(data))
        response_text = json.loads(res.text)
        # 查询出的flow id, name, flowType，并组装成一个dict， 和response对比
        SQL = 'select id, name,flow_type from merce_flow ORDER BY last_modified_time DESC LIMIT 8'
        flow_query_info = ms.ExecuQuery(SQL)
        flow_query_name = flow_query_info[0][1]  # flow name
        flow_query_id = flow_query_info[0][0]  # flow id
        flow_query_flow_type = flow_query_info[0][2]  # flow type
        self.assertEqual(res.status_code, 200, 'flow查询后返回的status_code不正确')
        self.assertEqual(response_text['content'][0]['id'], flow_query_id, 'flow查询ID不相等')
        self.assertEqual(response_text['content'][0]['name'], flow_query_name, 'flow查询name不相等')
        self.assertEqual(response_text['content'][0]['flowType'], flow_query_flow_type, 'flowflow_type不一致')
        time.sleep(5)

    def test_case02(self):
        """正常查询流程分页-LIKE"""

        data = {"fieldList": [{"fieldName": "parentId", "fieldValue": Flows_resourceid, "comparatorOperator": "EQUAL"},
                              {"fieldName": "name", "fieldValue": "%test%", "comparatorOperator": "LIKE"}],
                "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
        res = requests.post(url=query_flows_url, headers=get_headers(), data=json.dumps(data))
        response_text = json.loads(res.text)
        # 查询出flow id, name, flowType，并组装成一个dict， 和response对比
        SQL = 'select id, name,flow_type from merce_flow where name like "%test%" ORDER BY last_modified_time DESC LIMIT 8'
        flow_query_info = ms.ExecuQuery(SQL)
        flow_query_name = flow_query_info[0][1]  # flow name
        flow_query_id = flow_query_info[0][0]  # flow id
        flow_query_flow_type = flow_query_info[0][2]  # flow type
        self.assertEqual(res.status_code, 200, 'flow查询后返回的status_code不正确')
        self.assertEqual(response_text['content'][0]['id'], flow_query_id, 'flow查询ID不相等2')
        self.assertEqual(response_text['content'][0]['name'], flow_query_name, 'flow查询name不相等2')
        self.assertEqual(response_text['content'][0]['flowType'], flow_query_flow_type, 'flowflow_type不一致2')
        time.sleep(5)

    def test_case03(self):
        """正常创建流程-含有steps"""

        flow_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'tc_auto_df_source_source'
        flow_sink_dataset_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + 'tc_auto_df_source_sink_out'
        data = '{"name": "' + flow_name + '", "flowType": "dataflow", "resource": {"id": "' + Flows_resourceid + '"}, "steps": [{"id":"source_0","type":"source","x":140,"y":180,"name":"source_0","outputConfigurations":[{"id":"output","fields":[{"column":"id","alias":""},{"column":"name","alias":""},{"column":"age","alias":""}]}],"otherConfigurations":{"dataset":"' + left_age_dataset_name + '","datasetId":"' + left_age_dataset_id + '","schema":"' + idnameage_schema_name + '","schemaId":"' + idnameage_schema_id + '"}},{"id":"sink_0","type":"sink","x":366,"y":173,"name":"sink_0","inputConfigurations":[{"id":"input","fields":[{"column":"id"},{"column":"name"},{"column":"age"}]}],"outputConfigurations":null,"otherConfigurations":{"dataset":"' + flow_sink_dataset_name + '","schema":"' + idnameage_schema_name + '","schemaId":"' + idnameage_schema_id + '","type":"HDFS","format":"csv","separator":",","quoteChar":"\\"","escapeChar":"\\\\","path":"/tmp/py/out/source/auto/' + flow_sink_dataset_name + '","sql":"","table":"","specifiedStringColumnTypes":[{"name":"","dataType":"","length":""}],"driver":"","url":"","user":"","password":"","brokers":"","topic":"","groupId":"","partitionColumns":"","namespace":"","columns":"","description":"","expiredTime":"0","sliceTimeColumn":"","sliceType":"H","mode":"append","nullValue":""}}], "links": [{"source":"source_0","target":"sink_0","targetInput":"input"}],"tenant":{"id":"' + tenant_id + '"}}'
        res = requests.post(url=create_flows_url, headers=get_headers(), data=data)
        response_text = json.loads(res.text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_Type = flow_info[0][1]
        self.assertEqual(res.status_code, 200, 'flow创建后返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow创建后查询ID不相等')
        self.assertEqual(response_text["flowType"], flow_Type, 'flow创建后flow_type不一致')
        time.sleep(3)

    def test_case04(self):
        """根据名称查询流程"""
        # 该接口没有返回值
        res = requests.get(url=query_flowname_url, headers=get_headers())
        self.assertEqual(res.status_code, 204, 'flow根据name查询返回的status_code不正确')
        time.sleep(3)

    def test_case05(self):
        """根据名称和版本查询历史流程"""

        res = requests.get(url=query_flowname_version_url, headers=get_headers())
        response_text = json.loads(res.text)
        # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where name= "%s"and version= "%s"' % (
            query_flow_name, query_flow_version)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        self.assertEqual(res.status_code, 200, 'flow根据名称和版本查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, 'flow查询后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, 'flow查询后version不一致')
        time.sleep(3)

    def test_case06(self):
        """查询简化版流程"""
        # 断言只写了个200

        res = requests.get(url=query_flow_all_url, headers=get_headers())

        response_text = json.loads(res.text)
        print(response_text)
        self.assertEqual(res.status_code, 200, 'flow查询简化版流程查询返回的status_code不正确')
        time.sleep(3)

    def test_case07(self):
        """更新流程"""
        from basic_info.get_flow_body import get_flow_update_body
        data = get_flow_update_body()
        flow_body_id = flow_update_id
        res = requests.put(url=flow_update_url, data=json.dumps(data), headers=get_headers())
        response_text = json.loads(res.text)
        # 查询创建的flow version，并组装成一个dict， 和response对比
        SQL = 'SELECT version from merce_flow where id= "%s"  ORDER BY version desc' % (flow_body_id)
        flow_info = ms.ExecuQuery(SQL)
        flow_version = flow_info[0][0]
        self.assertEqual(res.status_code, 200, '更新流程返回的status_code不正确')
        self.assertEqual(response_text["version"], flow_version, 'flow更新流程后版本不一致')
        time.sleep(3)

    def test_case08(self):
        """根据老的版本查询历史流程"""

        res = requests.get(url=query_flow_history_version_url, headers=get_headers())
        response_text = json.loads(res.text)
        # 查询创建的flow id, version, 并组装成一个dict， 和response对比
        SQL = 'SELECT oid,version from merce_flow_history where name= "%s" and version<= "%s" ORDER BY version desc' % (
            query_flow_name, query_flow_version)
        flow_info = ms.ExecuQuery(SQL)
        flow_oid = flow_info[0][0]
        flow_version = flow_info[0][1]

        self.assertEqual(res.status_code, 200, 'flow根据老的版本查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_oid, 'flow根据老的版本查询历史流程后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, 'flow根据老的版本查询历史流程后version不一致')
        time.sleep(3)

    def test_case09(self):
        """根据老的id查询历史流程"""

        res = requests.get(url=query_flow_history_id_url, headers=get_headers())
        response_text = json.loads(res.text)
        # 查询创建的flow_id, flow_version, 并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where name= "%s" ORDER BY version desc' % (query_flow_name)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        self.assertEqual(res.status_code, 200, 'flow根据老的id查询历史流程查询返回的status_code不正确')
        self.assertEqual(response_text[0]["id"], flow_id, 'flow根据老的id查询历史流程后查询ID不相等')
        self.assertEqual(response_text[0]["version"], flow_version, 'flow根据老的id查询历史流程后version不一致')
        time.sleep(3)

    # def test_case10(self):
    #     """根据流程id和计划id查询执行历史"""
    #     # 测试未通过500 {"err":"null\n"}
    #
    #     res = requests.get(url=query_flow_flowAscheduler_id_url, headers=get_headers())
    #
    #     print(res.status_code, res.text)
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
        response_text = json.loads(res.text)
        # 查询创建的flow flow_id, flow_version，并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow_history where oid= "%s" and version= "%s"' % (
            flow_update_id, query_flow_version)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        self.assertEqual(res.status_code, 200, '根据老的版本查询流程查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, '根据老的版本查询流程后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, '根据老的版本查询流程后version不一致')
        time.sleep(3)

    # def test_case12(self):
    #     """更新版本"""
    #     # 测试未通过501 {"err":"Name Duplicated"}
    #     data = {"name": query_flow_name, "version": 13}
    #     res = requests.put(url=flow_name_version_update_url, data=json.dumps(data), headers=get_headers())
    #
    #     print(res.status_code, res.text)
    #     response_text = json.loads(res.text)
    #     # print(response_text)
    #     # 查询创建的flow id, name, type，并组装成一个dict， 和response对比
    #     # SQL = 'SELECT id,version from merce_flow_history where oid= "%s" and version= "%s"' % (
    #     # flow_update_id, query_flow_version)
    #     # print(SQL)
    #     # flow_info = ms.ExecuQuery(SQL)
    #     # flow_id = flow_info[0][0]
    #     # flow_version = flow_info[0][1]
    #     # # print(flow_id, flow_Type)
    #     # # print(type(response_text), response_text)
    #     # # print(response_text[0]["id"])
    #     # self.assertEqual(res.status_code, 200, '根据老的版本查询流程查询返回的status_code不正确')
    #     # self.assertEqual(response_text["id"], flow_id, '根据老的版本查询流程后查询ID不相等')
    #     # self.assertEqual(response_text["version"], flow_version, '根据老的版本查询流程后version不一致')
    #     time.sleep(3)

    # def test_case13(self):
    #     """根据id和版本更新流程"""
    # #     测试未通过500 {"err":"No origin bean specified\n"}
    #     data = {"id": flow_update_id, "version": 13}
    #     res = requests.put(url=flow_id_version_update_url, data=json.dumps(data), headers=get_headers())
    #     print(res.status_code, res.text)
    #     response_text = json.loads(res.text)
    #     time.sleep(3)

    # def test_case14(self):
    #     """更新执行计划"""
    # #     测试未通过
    #     now = int(round(time.time() * 1000))
    #     print(now)
    #     now_day = time.strftime("%Y%m%d", time.localtime())
    #     now_hour = time.strftime("%Y%m%d%H", time.localtime())
    #     print(now_day, now_hour)
    #     data = {
    #         "id": "cb0a37ea-de4a-495c-bae0-236fcbd08eaf",
    #         "name": "test_df_supplement",
    #         "scheduler": "scheduler",
    #         "schedulerConfigs": {
    #             "cron": 0,
    #             "startTime": now,
    #             "arguments": [],
    #             "properties": [{
    #                 "name": "all.debug",
    #                 "value": "false"
    #             }, {
    #                 "name": "all.dataset-nullable",
    #                 "value": "false"
    #             }, {
    #                 "name": "all.lineage.enable",
    #                 "value": "true"
    #             }, {
    #                 "name": "all.notify-output",
    #                 "value": "false"
    #             }, {
    #                 "name": "all.debug-rows",
    #                 "value": "20"
    #             }, {
    #                 "name": "dataflow.master",
    #                 "va lue": "yarn"
    #                 }, {
    #                 "name": "dataflow.deploy-mode",
    #                 "value": ["client", "cluster"]
    #             }, {
    #                 "name": "dataflow.queue",
    #                 "value": ["default"]
    #             }, {
    #                 "name": "dataflow.num-executors",
    #                 "value": "2"
    #             }, {
    #                 "name": "dataflow.driver-memory",
    #                 "value": "512M"
    #             }, {
    #                 "name": "dataflow.executor-memory",
    #                 "value": "1G"
    #             }, {
    #                 "name": "dataflow.executor-cores",
    #                 "value": "2"
    #             }, {
    #                 "name": "dataflow.verbose",
    #                 "value": "true"
    #             }, {
    #                 "name": "dataflow.local-dirs",
    #                 "value": ""
    #             }, {
    #                 "name": "dataflow.sink.concat-files",
    #                 "value": "true"
    #             }]
    #         }
    #     }
    #     res = requests.put(url=flow_update_schedulers_url, data=json.dumps(data), headers=get_headers())
    #
    #     print(res.status_code, res.text)
    #

    def test_case15(self):
        """根据id查询flow"""

        res = requests.get(url=flow_queryById_url, headers=get_headers())
        response_text = json.loads(res.text)
        print(res.status_code, res.text)
        # 查询创建的flow id, name，并组装成一个dict， 和response对比
        SQL = 'SELECT id,name from merce_flow where id= "%s"' % (flow_update_id)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_name = flow_info[0][1]
        self.assertEqual(res.status_code, 200, '根据id查询flow查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, '根据id查询flow后查询ID不相等')
        self.assertEqual(response_text["name"], flow_name, '根据id查询flow后name不一致')
        time.sleep(3)

    def test_case16(self):
        """根据id查询版本"""

        # data = {"id": flow_update_id, "version": 1}
        res = requests.get(url=flow_queryById_Version_url, headers=get_headers())
        response_text = json.loads(res.text)
        # print(res.status_code, response_text["version"])
        # 查询创建的flow id, version，并组装成一个dict， 和response对比
        SQL = 'SELECT id,version from merce_flow where id= "%s"' % (flow_update_id)
        flow_info = ms.ExecuQuery(SQL)
        flow_id = flow_info[0][0]
        flow_version = flow_info[0][1]
        self.assertEqual(res.status_code, 200, '根据id查询版本查询返回的status_code不正确')
        self.assertEqual(response_text["id"], flow_id, '根据id查询版本后查询ID不相等')
        self.assertEqual(response_text["version"], flow_version, '根据id查询版本后version不一致')
        time.sleep(3)

    def test_case17(self):
        """查询运行时的一些属性"""
        # 断言只断言了200
        res = requests.get(url=query_flow_id_runP_url, headers=get_headers())
        print(res.status_code, res.text)
        self.assertEqual(res.status_code, 200, '查询运行时的一些属性查询返回的status_code不正确')
        time.sleep(3)

    # def test_case18(self):
    #     """根据用户名更新流程"""
    #     # 测试未通过返回500 {'err': 'null\n'}
    #     from basic_info.get_flow_body import get_flow_update_body
    #     data = get_flow_update_body()
    #     print(data)
    #     res = requests.put(url=flow_updateByName_url, data=json.dumps(data), headers=get_headers())
    #     response_text = json.loads(res.text)
    #     print(res.status_code, response_text)
    #     # 查询创建的flow id, version，并组装成一个dict， 和response对比
    #     # SQL = 'SELECT id,version from merce_flow where id= "%s"' % (flow_update_id)
    #     # flow_info = ms.ExecuQuery(SQL)
    #     # flow_id = flow_info[0][0]
    #     # flow_version = flow_info[0][1]
    #     # self.assertEqual(res.status_code, 200, '根据id查询版本查询返回的status_code不正确')
    #     # self.assertEqual(response_text["id"], flow_id, '根据id查询版本后查询ID不相等')
    #     # self.assertEqual(response_text["version"], flow_version, '根据id查询版本后version不一致')
    #     # time.sleep(3)

    def test_case19(self):
        """导出流程-Flows"""
        # 无返回值，日志中可查看导出成功日志platform/logs/woven-server/woven-server.log
        from basic_info.get_auth_token import get_headers_flow
        from urllib import parse

        params = parse.urlencode({"ids": flow_update_id, "withDependencies": "true"})
        res = requests.get(url=flow_export_url, params=params, headers=get_headers_flow())
        # response_text = json.loads(res.text)
        # print(res.status_code, res.url)
        self.assertEqual(res.status_code, 200, '导出流程返回的status_code不正确')
        time.sleep(3)

    def test_case20(self):
        """清理status"""
        # 无返回值，日志中可查看clean成功日志platform/logs/woven-server/woven-server.log
        data = {"id": flow_update_id}
        res = requests.post(url=flow_clean_status_url, data=json.dumps(data), headers=get_headers())
        # response_text = json.loads(res.text)
        print(res.status_code, res.url, res.text)
        self.assertEqual(res.status_code, 204, '清理status返回的status_code不正确')
        time.sleep(3)

    def test_case21(self):
        """根据id删除流程"""
        # 无返回值
        # 根据id删除流程
        flow_delete_ById_url = '%s/api/flows/%s' % (MY_LOGIN_INFO["HOST"], create_flow())
        res = requests.delete(url=flow_delete_ById_url, headers=get_headers())
        # response_text = json.loads(res.text)
        # print(res.status_code, res.url, res.text)
        self.assertEqual(res.status_code, 204, '根据id删除流程返回的status_code不正确')

        time.sleep(3)

    def test_case22(self):
        """根据id删除流程-project"""
        # 无返回值
        res = requests.delete(url=flow_delete_ByProjectId_url, headers=get_headers())
        # response_text = json.loads(res.text)
        # print(res.status_code, res.url, res.text)
        self.assertEqual(res.status_code, 204, '根据id删除流程-project返回的status_code不正确')
        time.sleep(3)

    def test_case23(self):
        """根据id批量删除流程"""
        # 无返回值,已加判断
        from basic_info.get_flow_body import create_flow
        id1 = create_flow()
        time.sleep(2)
        id2 = create_flow()

        # delete_flow_id2 = create_flow()
        # delete_flow_id2 = delete_flow_id2['id']
        data = [id1, id2]
        print(data)
        res = requests.post(url=flow_delete_removeList_url, data=json.dumps(data), headers=get_headers())
        # response_text = json.loads(res.text)
        print(res.status_code, res.url, res.text)
        self.assertEqual(res.status_code, 204, '根据id批量删除流程返回的status_code不正确')
        time.sleep(3)
        # 查询数据库的flow_id来判断是否批量删除成功
        try:
            sql = 'SELECT id from merce_flow where id in ("%s", "%s")' % (id1, id2)
            flow_delete_info = ms.ExecuQuery(sql)
        except Exception as e:
            print("flow数据查询出错:%s" % e)
        else:
            if not flow_delete_info:
                print('批量删除flow成功')
                # flow_delete_id = list(flow_delete_info[0])
            else:
                self.assertEqual(1, 0, '批量删除flow失败')

    def test_case24(self):
        """根据id批量删除流程-project"""
        # 无返回值
        data1 = ['1', '2']
        res = requests.post(url=flow_delete_removeListProject_url, data=json.dumps(data1), headers=get_headers())
        # response_text = json.loads(res.text)
        print(res.status_code, res.url, res.text)
        self.assertEqual(res.status_code, 204, '根据id批量删除流程-project返回的status_code不正确')
        time.sleep(3)

# if __name__ == '__main__':
#     unittest.main()
