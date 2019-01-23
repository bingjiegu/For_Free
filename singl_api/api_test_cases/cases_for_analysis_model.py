from basic_info.get_auth_token import get_headers
import unittest, time, json, requests, random
from basic_info.setting import preProcessFlowId, preProcessFlowName, processDataId
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO
from basic_info.format_res import dict_res

# ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
class CreateAnalysisModel(unittest.TestCase):
    from basic_info.url_info import create_analysis_model

    def setUp(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

    def tearDown(self):
        pass

    def test_create_analysis_model1(self):
        """创建分析模板, 不添加flow"""
        model_name = 'api_test_analysis_model' + str(random.randint(0, 99999))
        data = {"name": model_name,
                # "preProcessFlowId": preProcessFlowId,
                # "preProcessFlowName": preProcessFlowName,
                "processDataId": processDataId,
                "processDataType": "Dataset"
                }
        response = requests.post(url=self.create_analysis_model, headers = get_headers(), json = data)
        self.assertEqual(response.status_code, 201, '分析模板创建失败')
        self.assertIsNotNone(response.json(), '分析模板创建后返回None')

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
        model_id_info = self.ms.ExecuQuery(sql)
        model_id = model_id_info[0]["id"]
        data = []
        data.append(model_id)
        response = requests.post(url=zmod_removeList_url, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 204, '删除分析模板失败')
        print(response.status_code)


class QueryAnalysisRule(unittest.TestCase):
    def test_query_analysis_rule(self):
        """查看单个模板的分析规则"""
        sql = 'select id from merce_model where name like "api_test_analysis_model%" ORDER BY create_time desc limit 1'
        model_id_info = self.ms.ExecuQuery(sql)
        model_id = model_id_info[0]["id"]
        query_zmod_rule = "%s/api/woven/zmodrules/%s/detailslist" % (MY_LOGIN_INFO["HOST"], model_id)
        data = {"fieldList":[],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        response = requests.post(url=query_zmod_rule, headers=get_headers(), json=data)
        self.assertEqual(response.status_code, 200, '查询分析模板规则的接口调用失败')
        print(response.status_code, response.json())
        time.sleep(5)


if __name__ == '__main__':
    unittest.main()