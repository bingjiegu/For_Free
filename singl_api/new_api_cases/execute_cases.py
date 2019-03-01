
import os
from openpyxl import load_workbook
import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


# 判断请求方法，并根据不同的请求方法进行调用不同的处理方式
class TES:
    case_table = load_workbook(ab_dir("api_cases.xlsx"))
    case_table_sheet = case_table.get_sheet_by_name('api_cases')
    all_rows = case_table_sheet.max_row

    def deal_request_method(self):
        for i in range(2, self.all_rows+1):
            request_method = self.case_table_sheet.cell(row=i, column=5).value
            request_url = self.case_table_sheet.cell(row=i, column=6).value
            request_data = dict_res(self.case_table_sheet.cell(row=i, column=8).value)
            # 请求方法转大写
            if request_method:
                request_method_upper = request_method.upper()
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    res = self.POST_request_result_check(request_url, get_headers(), request_data)
                    # 写入接口返回的响应状态码
                    self.case_table_sheet.cell(row=i, column=10, value=res[0])
                    # 写入接口返回的text
                    self.case_table_sheet.cell(row=i, column=13, value=res[1])
                    # 保存表格
                    self.case_table.save(ab_dir("api_cases.xlsx"))

                elif request_method_upper == 'GET':
                    self.GET_request_result_check(request_url, get_headers())
                elif request_method_upper == 'PUT':
                    self.PUT_request_result_check()
                elif request_method_upper == 'DELETE':
                    self.DELETE_request_result_check()
                else:
                    print('请求方法%s不在处理范围内' % request_method)

    def POST_request_result_check(self, url, headers, data):
        response = requests.post(url=url, headers=headers, json=data)
        # 返回响应状态码和响应text
        return response.status_code, response.text


    def GET_request_result_check(self, url, headers):
        print('get')

    def DELETE_request_result_check(self):
        print('delete')

    def PUT_request_result_check(self):
        print('put')



g = TES()
print(g.deal_request_method(), )