# coding:utf-8
from util.send_mail import main3
import datetime
from new_api_cases.execute_cases import deal_request_method
from new_api_cases.execute_cases import CheckResult
from basic_info.setting import HOST_189
from util.clean_test_data import CleanData

print('------开始执行用例-------')
print('用例执行环境：', HOST_189)
start_time = datetime.datetime.now()
print('开始时间：', start_time)
print('------开始执行api case------')
# 执行API用例并对比结果
deal_request_method()
CheckResult().deal_result()
stop_time = datetime.datetime.now()
print('结束时间：', stop_time)
print('耗时:', stop_time-start_time)
print('清理测试数据')
CleanData().clean_datasource_test_data()

# 发送邮件
print('开始发送邮件')
# main3(HOST_189)













