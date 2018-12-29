import unittest
import time
import HTMLTestRunner
from send_mail import main2, main3
from basic_info.get_auth_token import get_headers
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
import threading
# from newSuite import NewSuite

testcase = unittest.TestSuite()
discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for_*.py')

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

# 需要执行的脚本
obj = GetCheckoutDataSet()
sink_dataet_json = obj.get_json()
main3(report_path=report_path)
threading.Timer(1500, get_headers()).start()




