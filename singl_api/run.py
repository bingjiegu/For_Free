# coding:utf-8
import unittest
import time
import HTMLTestRunner
from send_mail import main2, main3
from basic_info.get_auth_token import get_headers
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
import threading
import datetime
from new_api_cases.execute_cases import deal_request_method, CheckResult
# from newSuite import NewSuite

# 添加用例集的API用例。暂停
# testcase = unittest.TestSuite()
# discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for_*.py')
#
# for test_suite in discover:
#     for test_case in test_suite:
#         # print(test_case)
#         testcase.addTest(test_case)
# filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
# report_path = 'E:\Reports\\' + filename
# # report_path = '/root/gbj/Reports/' + filename  # 192.168.1.87环境Jenkins使用
# fp = open(report_path, 'wb')
# runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='覆盖dataset,schema,schedulers,execution等测试场景')
# print('开始执行API自动化脚本')

# print('start_time:', start_time)
# runner.run(testcase)
# fp.close()
print('------开始执行用例-------')
start_time = datetime.datetime.now()
print('------开始执行flow用例------')
# 执行flow用例
obj = GetCheckoutDataSet()
sink_dataet_json = obj.get_json()
print('------开始执行api case------')
# 执行API用例
deal_request_method()
# 对比API用例结果
CheckResult().deal_result()
# 发送邮件
main3()
stop_time = datetime.datetime.now()
print('耗时:', stop_time-start_time)
# threading.Timer(1500, get_headers()).start()
# print('重新发送一次TOKEN')











