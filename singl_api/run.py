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


obj = GetCheckoutDataSet()
sink_dataet_json = obj.get_json()
main3(report_path=report_path)




