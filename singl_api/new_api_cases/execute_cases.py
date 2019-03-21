# coding:utf-8
import os
import re
from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.format_res import get_time
from basic_info.timestamp_13 import timestamp_to_13
import random

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name('tester')
all_rows = case_table_sheet.max_row


# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method():
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        request_url = case_table_sheet.cell(row=i, column=5).value
        # request_data = case_table_sheet.cell(row=i, column=6).value
        data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(data)
        key_word = case_table_sheet.cell(row=i, column=3).value
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            # 根据不同的请求方法，进行分发
            if request_method_upper == 'POST':
                # 调用post方法发送请求
                post_request_result_check(row=i, column=8, url=request_url, headers=get_headers(),
                                                data=request_data, table_sheet_name=case_table_sheet)

            elif request_method_upper == 'GET':
                # 调用GET请求
                get_request_result_check(url=request_url, headers=get_headers(), data=request_data,
                                         table_sheet_name=case_table_sheet, row=i, column=8)

            elif request_method_upper == 'PUT':
                put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8)

            elif request_method_upper == 'DELETE':
                delete_request_result_check()

            else:
                print('请求方法%s不在处理范围内' % request_method)
        else:
            print('第 %d 行请求方法为空' % i)
    #  执行结束后保存表格
    case_table.save(ab_dir("api_cases.xlsx"))


# POST请求
def post_request_result_check(row, column, url, headers, data, table_sheet_name):

    if isinstance(data, str):
        #  SQL语句作为参数，需要先将SQL语句执行，数据库查询返回数据作为接口要传递的参数
        if data.startswith('select'):  # 后续根据需要增加其他select内容，如name或者其他？？？？？？
            # print('data startswith select:', data)
            data_select_result = ms.ExecuQuery(data)
            # print(data_select_result)
            datas = []
            if data_select_result:
                try:
                    for i in range(len(data_select_result)):
                        datas.append(data_select_result[i]["id"])
                except:
                    print('请确认第%d行SQL语句' % row)
                else:
                    response = requests.post(url=url, headers=headers, json=datas)
                    # 将返回的status_code和response.text分别写入第10列和第14列
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                print('第%d行参数查询无结果' % row)
        # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
        elif data.startswith('{') and data.endswith('}'):
            print('data startswith {:', data)
            data_dict = dict_res(data)
            print(data_dict)
            response = requests.post(url=url, headers=headers, json=data_dict)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        # 列表作为参数， 如["9d3639f0-02bc-44cd-ac71-9a6d0f572632"]
        elif data.startswith('[') and data.endswith(']'):
            # print('data startswith [:', data)
            data_list = dict_res(data)
            # print(type(data_list))
            if data:
                response = requests.post(url=url, headers=headers, json=data_list)
                # print(response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                print('请先确认第%d行list参数值' % row)
        else:
                print('第%d行参数不是以startswith或者{,[开头，请先确认参数内容' % row)
    else:
        print('请确认第%d行的data形式' % row)


# GET请求
def get_request_result_check(url, headers, data, table_sheet_name, row, column):
    # GET请求需要从parameter中获取参数,并把参数拼装到URL中，
    if data:
        # 分割参数，分割后成为一个列表['61bf20da-f42c-4b35-9142-0fc2a7664e3e', '2']
        parameters = data.split('&')
        # 处理存在select语句中的参数，并重新赋值后传递给URL
        for i in range(len(parameters)):
            if parameters[i].startswith('select id from'):
                select_result = ms.ExecuQuery(parameters[i])
                parameters[i] = select_result[0]["id"]
            elif parameters[i].startswith('select name from'):
                select_result = ms.ExecuQuery(parameters[i])
                parameters[i] = select_result[0]["name"]

        # 判断URL中需要的参数个数，并比较和data中的参数个数是否相等
        if len(parameters) == 1:
            url_new = url.format(parameters[0])
            response = requests.get(url=url_new, headers=headers)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif len(parameters) == 2:
            url_new = url.format(parameters[0], parameters[1])
            response = requests.get(url=url_new, headers=headers)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif len(parameters) == 3:
            url_new = url.format(parameters[0], parameters[1], parameters[2])
            response = requests.get(url=url_new, headers=headers)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            print('请确认第%d行parameters' % row)
    # GET 请求参数写在URL中，直接发送请求
    else:
        response = requests.get(url=url, headers=headers)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)


# PUT请求
def put_request_result_check(url, row, data, table_sheet_name, column):
    # 分隔参数
    parameters = data.split('&')
    # 拼接URL
    new_url = url.format(parameters[0])
    # 发送的参数体
    parameters_data = parameters[-1]
    if parameters_data.startswith('{'):
        response = requests.put(url=new_url, headers=get_headers(), json=dict_res(parameters_data))
        write_result(table_sheet_name, row, column, response.status_code)
        write_result(table_sheet_name, row, column+4, response.text)
    else:
        print('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)

def delete_request_result_check():
    print('delete')


#  写入返回结果
def write_result(sheet, row, column, value):
    sheet.cell(row=row, column=column, value=value)

def deal_parameters(data):
    if data:
        if '随机数' in data:
            # print(data)
            new_data = data.replace('随机数', str(random.randint(0, 99999999)))
            return new_data
        else:
            return data




deal_request_method()
#
# url = case_table_sheet.cell(row=2,column=7).value
# data = case_table_sheet.cell(row=2, column=8).value
# key_word = case_table_sheet.cell(row=2, column=5).value
# # post_request_result_check(key_word, row, column, url, headers, data, table_sheet_name)
# put_request_result_check( row=2, data=data, table_sheet_name=case_table_sheet, column=10)

# case_table.save(ab_dir("api_cases.xlsx"))

# print(ab_dir("api_cases.xlsx"))