import unittest
import time
import HTMLTestRunner
from send_mail import main2, main3
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
from newSuite import NewSuite

# test
# suit = NewSuite.Suite()
# discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for_login_api.py')
# start_time = time.strftime('%Y%m%d_%H_%M_%S', time.localtime())
# for test_suite in discover:
#     for case in test_suite:
#         suit.addTest(case)
# filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
# report_path = 'E:\Reports\\' + filename
# fp = open(report_path, 'wb')
# runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='覆盖dataset,schema,schedulers,execution等测试场景')
# runner.run(suit)
#
#
# # main3(report_path)



