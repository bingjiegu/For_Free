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


print('开始执行test')