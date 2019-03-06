
import os
from openpyxl import load_workbook
import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name('tester')
all_rows = case_table_sheet.max_row


# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method():
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=6).value
        request_url = case_table_sheet.cell(row=i, column=7).value
        request_data = dict_res(case_table_sheet.cell(row=i, column=8).value)
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            # 根据不同的请求方法，进行分发
            if request_method_upper == 'POST':
                # 调用post方法发送请求
                res = post_request_result_check(request_url, get_headers(), request_data)
                # 写入接口返回的响应状态码, 写入接口返回的text
                write_result(case_table_sheet, i, 10, res[0])
                write_result(case_table_sheet, i, 14, res[1])

            elif request_method_upper == 'GET':
                # 调用GET请求
                res = get_request_result_check(request_url, get_headers())
                write_result(case_table_sheet, i, 10, res[0])
                write_result(case_table_sheet, i, 14, res[1])

            elif request_method_upper == 'PUT':
                res = put_request_result_check(request_url, get_headers(), request_data)
                write_result(case_table_sheet, i, 10, res[0])
                write_result(case_table_sheet, i, 14, res[1])

            elif request_method_upper == 'DELETE':
                delete_request_result_check()

            else:
                print('请求方法%s不在处理范围内' % request_method)
        else:
            print('第 %d 行请求方法为空' % i)
    #  执行结束后保存表格
    case_table.save(ab_dir("api_cases.xlsx"))


# POST请求
def post_request_result_check(url, headers, data):
    response = requests.post(url=url, headers=headers, json=data)
    # 返回响应状态码和响应text
    return response.status_code, response.text


# GET请求
def get_request_result_check(url, headers):
    response = requests.get(url=url, headers=headers)
    return response.status_code, response.text


def delete_request_result_check():
    print('delete')


# PUT请求
def put_request_result_check(url, headers, data):
    response = requests.put(url=url, headers=headers, json=data)
    return response.status_code, response.text


#  写入返回结果
def write_result(sheet, row, column, value):
    sheet.cell(row=row, column=column, value=value)
    return



deal_request_method()