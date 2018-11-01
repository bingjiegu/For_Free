import unittest
import time
import HTMLTestRunnerCN
import HTMLTestRunner
from send_mail import main2
#
# suite = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='platform_api*.py')
# # filename = time.strftime('%Y%m%d_%H_%M_%S', time.localtime(time.time()))+'.html'
# filename = 'Test_Report.html'
# fp = open('E:\Reports\Test_Report.html', 'wb')
# HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='包含创建dataset,schema,flow,schedulers的测试用例').run(suite)
# fp.close()
#
# main2()
#
testcase = unittest.TestSuite()
discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='*.py')
for test_suite in discover:
    for test_case in test_suite:
        print(test_case)
        testcase.addTest(test_case)

report_path = 'E:\Reports\Test_report.html'
fp = open(report_path, 'wb')
runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='接口自动化测试用例，目前包括dataset 和 schema')
runner.run(testcase)
fp.close()


main2()

