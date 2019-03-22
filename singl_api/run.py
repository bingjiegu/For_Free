# coding:utf-8
import unittest
import time
import HTMLTestRunner
from send_mail import main2, main3
from basic_info.get_auth_token import get_headers
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
import threading
from new_api_cases.check_result import CheckResult
import datetime
# from newSuite import NewSuite

testcase = unittest.TestSuite()
discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for_*.py')

for test_suite in discover:
    for test_case in test_suite:
        # print(test_case)
        testcase.addTest(test_case)
filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
report_path = 'E:\Reports\\' + filename
# report_path = '/root/gbj/Reports/' + filename  # 192.168.1.87环境Jenkins使用
fp = open(report_path, 'wb')
runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='覆盖dataset,schema,schedulers,execution等测试场景')
print('开始执行API自动化脚本')
start_time = datetime.datetime.now()
print('start_time:', start_time)
runner.run(testcase)
fp.close()
print('自动化脚本执行结束，开始执行flow用例')
# 需要执行的脚本
obj = GetCheckoutDataSet()
sink_dataet_json = obj.get_json()

print('开始执行excel 版本api case')
CheckResult().deal_result()
main3(report_path=report_path)
stop_time = datetime.datetime.now()
print('stop_time:', stop_time)
print('耗时:', stop_time-start_time)
# threading.Timer(1500, get_headers()).start()
# print('重新发送一次TOKEN')











