import unittest
import requests
import time
from basic_info.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG
from basic_info.format_res import dict_res
from basic_info.ready_dataflow_data import get_dataflow_data
from basic_info.url_info import create_scheduler_url
from basic_info.get_auth_token import get_headers
from basic_info.setting import MY_LOGIN_INFO2

class ExecuteWeirdDataflow(unittest.TestCase):

    def setUp(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
        self.expected_result = ['[{"name":"james","id":"6","age":"50"}]', '[{"name":"xiaowang","id":"3","age":"30"}]', '[{"name":"xiaoming","id":"1","age":"18"}]', '[{"name":"tyest","id":"4","age":"12"}]', '[{"name":"xiaohong","id":"2","age":"20"}]', '[{"name":"空","id":"5","age":"空"}]']

    def tearDown(self):
        pass

    def create_scheduler(self):
        data = get_dataflow_data('tc_auto_df_sink_hdfs_path使用$进行分区、使用sliceTimeColumn1545633382888')
        res = requests.post(url=create_scheduler_url, headers=get_headers(), json=data)
        self.assertEqual(res.status_code, 201)
        self.assertNotEqual(res.json().get('id', 'scheduler创建可能失败了'), 'scheduler创建可能失败了')
        scheduler_id = res.json()['id']
        print('---------scheduler_id-------', scheduler_id)
        return scheduler_id

    def get_execution_info(self):
        scheduler_id = self.create_scheduler()
        time.sleep(5)
        e_status_format = {'type': 'READY'}
        while e_status_format['type'] in ("READY", "RUNNING"):
            time.sleep(5)
            execution_sql = 'select id, status, flow_id , flow_scheduler_id from merce_flow_execution where flow_scheduler_id = "%s" ' % scheduler_id
            select_result = self.ms.ExecuQuery(execution_sql)
            e_status = select_result[0]["status"]
            e_status_format = dict_res(e_status)
            print(e_status_format)
        self.assertEqual(e_status_format['type'], 'SUCCEEDED')
        return select_result

    def get_dataset_id(self):
        """获取execution的id和状态, 最终返回execution执行成功后的dataset id """
        e_info = self.get_execution_info()
        data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_info[0]["id"]
        data_json = self.ms.ExecuQuery(data_json_sql)
        sink_dataset_list = []
        for n in range(len(data_json)):
            sink_dataset = data_json[n]["dataset_json"]  # 返回结果为元祖
            sink_dataset_id = dict_res(sink_dataset)["id"]  # 取出json串中的dataset id
            sink_dataset_list.append(sink_dataset_id)
        print('----------sink_dataset_list----------', sink_dataset_list)
        return sink_dataset_list

    def test_check_result(self):
        ''' 返回多dataset且ID会变，对该flow的校验 '''
        sink_dataset_list = self.get_dataset_id()
        L = []
        for dataset_id in sink_dataset_list:
            priview_url = "%s/api/datasets/%s/preview?rows=5000&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (MY_LOGIN_INFO2["HOST"], dataset_id)
            result = requests.get(url=priview_url, headers=get_headers())
            L.append(result.text)
        result = [ i for i in self.expected_result if i not in L]
        self.assertEqual(len(self.expected_result), len(L))
        self.assertEqual(result, [])

if __name__  == '__main__':
    unittest.main()




