from basic_info.get_auth_token import get_headers
import unittest, time, json, requests, random
from basic_info.setting import preProcessFlowId, preProcessFlowName, processDataId
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO
from basic_info.timestamp_13 import timestamp_to_13

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class CreateAnalysisModel(unittest.TestCase):
    from basic_info.url_info import create_analysis_model

    def test_create_analysis_model1(self):
        """创建分析模板, 不添加flow"""
        model_name = 'api_test_analysis_model' + str(random.randint(0, 99999))
        data = {"name": model_name,
                # "preProcessFlowId": preProcessFlowId,
                # "preProcessFlowName": preProcessFlowName,
                "processDataId": processDataId,
                "processDataType": "Dataset"
                }
        response = requests.post(url=self.create_analysis_model, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 201, '分析模板创建失败')
        self.assertIsNotNone(response.json(), '分析模板创建后返回None')
        return response.json()["id"]

    def test_Add_Model_Rule_For_Analysis_model(self):
        rule_name = "model-rule" + str(timestamp_to_13(digits=13))
        modelId = CreateAnalysisModel().test_create_analysis_model1()
        data = {"name": rule_name,
                "modelId": modelId,
                "dataId": "*",
                "ruleId": "e0d2c763-0196-4b71-9e33-da44f3cd5c1d",  # rule id 后续改为创建rule后返回
                "ruleName": "digit format",
                "priority": 1,
                "inputParams":
                    {"outputGroup": {"0": {"name": "outputFields", "value": "*"},
                                    "1": {"name": "qualityType", "value": "normal"},
                                    "2": {"name": "outputLimit", "value": "1000000"}},
               "inputGroup":{"0": {"name": "inputELColumns", "value": "*"},
                             "1": {"name": "customExpression", "value": "/\\d/"}}}}

        url = "%s/api/woven/zmodrules/%s" % (MY_LOGIN_INFO["HOST"], modelId)
        response = requests.post(url=url, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 201, '分析模板规则添加失败')

    def test_create_analysis_model2(self):
        """创建分析模板, 添加flow"""
        model_name = 'api_test_analysis_model' + str(random.randint(0, 99999))
        data = {"name": model_name,
                "preProcessFlowId": preProcessFlowId,
                "preProcessFlowName": preProcessFlowName,
                "processDataId": processDataId,
                "processDataType": "Dataset"
                }
        response = requests.post(url=self.create_analysis_model, headers = get_headers(), json = data)
        self.assertEqual(response.status_code, 201, '分析模板创建失败')
        self.assertIsNotNone(response.json(), '分析模板创建后返回None')
        time.sleep(5)

    def test_delete_analysis_model(self):
        """删除分析规则"""
        from basic_info.url_info import zmod_removeList_url
        sql = 'select id from merce_model where name like "api_test_analysis_model%" ORDER BY create_time desc limit 1'
        model_id_info = ms.ExecuQuery(sql)
        model_id = model_id_info[0]["id"]
        data = []
        data.append(model_id)
        response = requests.post(url=zmod_removeList_url, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 204, '删除分析模板失败')
        print(response.status_code)


class QueryAnalysisRule(unittest.TestCase):
    def test_query_analysis_rule(self):
        """查看单个模板的分析规则"""
        query_zmod_rule = "%s/api/woven/zmodrules/%s/detailslist" % (MY_LOGIN_INFO["HOST"], CreateAnalysisModel().test_create_analysis_model1())
        data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
        response = requests.post(url=query_zmod_rule, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 200, '查询分析模板规则的接口调用失败')



if __name__ == '__main__':
    unittest.main()