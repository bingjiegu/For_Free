import unittest
import time
import HTMLTestRunnerCN
import HTMLTestRunner
from send_mail import main2, main3
from api_test_cases.get_execution_output_json import GetCheckoutDataSet

testcase = unittest.TestSuite()

discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for*.py')

for test_suite in discover:
    for test_case in test_suite:
        # print(test_case)
        testcase.addTest(test_case)
filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
report_path = 'E:\Reports\\' + filename
fp = open(report_path, 'wb')
runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='覆盖dataset,schema,schedulers,execution等测试场景')
runner.run(testcase)
fp.close()



# sink_dataet_json = [{'flow_id': '35033c8d-fadc-4628-abf9-6803953fba34', 'execution_id': '39954be8-900a-4466-bc2e-05e379697fef', 'flow_scheduler_id': '8cf78c22-a561-4e5b-9c1c-b709ae8a51fe', 'e_final_status': 'SUCCEEDED', 'o_dataset': 'b896ff9d-691e-4939-a860-38eb828b1ad2'}, {'flow_id': 'f2677db1-6923-42a1-8f18-f8674394580a', 'execution_id': 'a38d303f-5bf5-441b-831c-92df5a9b7299', 'flow_scheduler_id': '65d1ca0a-4f0d-4680-b667-291ca412bdb2', 'e_final_status': 'SUCCEEDED', 'o_dataset': 'b896ff9d-691e-4939-a860-38eb828b1ad2'}]

obj = GetCheckoutDataSet()
sink_dataet_json = obj.get_json()
main3(report_path=report_path)




