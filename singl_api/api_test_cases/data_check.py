from basic_info.setting import flow_id_json
import unittest
from api_test_cases.get_execution_output_json import GetCheckoutDataSet


class TT(unittest.TestCase):
    def test_1(self):
        print('\n')
        print("--------开始获取result---------------")
        actual_json = GetCheckoutDataSet().get_json()
        self.assertEqual(actual_json, flow_id_json, "同一个flow的execution，实际结果和预期结果不一致")
