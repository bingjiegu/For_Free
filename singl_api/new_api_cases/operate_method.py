# coding:utf-8
from new_api_cases.GET_requests import get_request_result_check
from new_api_cases.POST_requests import post_request_result_check
from new_api_cases.PUT_requests import put_request_result_check
from new_api_cases.DELETE_requests import delete_request_result_check
from new_api_cases.deal_parameters import deal_parameters
from basic_info.get_auth_token import get_headers_root,get_headers
import os
from util.get_host import url_host

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method(table_name, case_table, case_table_sheet, all_rows, ms, jar_dir):
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        old_request_url = case_table_sheet.cell(row=i, column=5).value
        request_url = deal_parameters(old_request_url)
        # print(request_url, type(request_url))
        host = url_host(request_url)
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data)
        api_name = case_table_sheet.cell(row=i, column=1).value
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            if api_name == 'tenants':  # 租户的用例需要使用root用户登录后操作
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, column=8, url=request_url, host=host, headers=get_headers_root(host),
                                              data=request_data, case_table_sheet=case_table_sheet, ms=ms,jar_dir=jar_dir)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, host=host, headers=get_headers_root(host), data=request_data,
                                             case_table_sheet=case_table_sheet, row=i, column=8, ms=ms)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, host=host, row=i, data=request_data,
                                             table_sheet_name=case_table_sheet, column=8, headers=get_headers_root(host),ms=ms)

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(request_url, request_data, case_table_sheet=case_table_sheet, row=i,
                                                column=8, headers=get_headers_root(host), ms=ms)
                else:
                    print('请求方法%s不在处理范围内' % request_method)
            else:
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, host=host, column=8, url=request_url, headers=get_headers(host),
                                                    data=request_data, case_table_sheet=case_table_sheet, ms=ms,jar_dir=jar_dir)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, host=host, headers=get_headers(host), data=request_data,
                                             case_table_sheet=case_table_sheet, row=i, column=8, ms=ms)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, host=host, row=i, data=request_data,
                                             case_table_sheet=case_table_sheet, column=8, headers=get_headers(host), ms=ms)

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(url=request_url, data=request_data,case_table_sheet=case_table_sheet,
                                                row=i, column=8, headers=get_headers(host), ms=ms)

                else:
                    print('请求方法%s不在处理范围内' % request_method)
        else:
            print('第 %d 行请求方法为空' % i)
    #  执行结束后保存表格
    case_table.save(ab_dir(table_name))


