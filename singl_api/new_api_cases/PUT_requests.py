from new_api_cases.clean_then_write_result import *
from util.format_res import dict_res
import requests


# PUT请求
def put_request_result_check(url, host, row, data, table_sheet_name, column, headers, ms):
    if data and isinstance(data, str):
        if '&' in data:
            # 分隔参数
            parameters = data.split('&')
            # 拼接URL
            new_url = url.format(parameters[0])
            # print(new_url)
            # print(parameters)
            # 发送的参数体
            parameters_data = parameters[-1]
            if parameters_data.startswith('{'):
                response = requests.put(url=new_url, headers=headers, json=dict_res(parameters_data))
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column+4, response.text)
            else:
                print('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)
        else:
            if data.startswith('select id'):
                result = ms.ExecuQuery(data)
                new_data = result[0]["id"]
                # print(new_data, type(new_data))
                new_url = url.format(new_data)
                # print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                # print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('{') and data.endswith('}'):
                response = requests.put(url=url, headers=headers, data=data)
                # print(response.status_code, response.text)
                # print(response.url, response.content)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('[') and data.endswith(']'):
                pass
            else:
                new_url = url.format(data)
                # print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                # print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
    else:
        print('第%s行的参数为空或格式异常' % row)
