from basic_info.setting import flow_id_json
import unittest
from api_test_cases.get_execution_output_json import GetCheckoutDataSet


class CheckOutPutData(unittest.TestCase):
    """execution 预期执行结果和实际执行结果对比"""

    def test_01(self):
        print('\n')
        print("--------开始获取result---------------")
        actual_json = GetCheckoutDataSet().get_json()
        self.assertEqual(actual_json, flow_id_json, "同一个flow的execution，实际结果和预期结果不一致")
