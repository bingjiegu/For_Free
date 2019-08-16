# coding:utf-8
import os
from openpyxl import load_workbook
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
from new_api_cases.operate_method import deal_request_method
from new_api_cases.check_result import CheckResult


table_names = ["api_cases_83.xlsx", "api_cases_57.xlsx"]
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
jar_dir = ab_dir('woven-common-3.0.jar')

class ExecuteCases:
    def __init__(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

    def execute(self):

        for table_name in table_names:
            case_table = load_workbook(ab_dir(table_name))
            case_table_sheet = case_table.get_sheet_by_name('tester')
            all_rows = case_table_sheet.max_row
            # 根据请求方法进行分发处理
            deal_request_method(table_name, case_table, case_table_sheet, all_rows, self.ms, jar_dir)
            # 对比结果
            CheckResult(table_name, case_table, case_table_sheet, all_rows).deal_result()

print(ExecuteCases().execute())


