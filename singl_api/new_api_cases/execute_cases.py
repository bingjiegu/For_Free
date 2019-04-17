# coding:utf-8
import os
import time
from selenium import webdriver

from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers, get_headers_root,get_auth_token
from basic_info.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from basic_info.Open_DB import MYSQL
from basic_info.setting import HOST_189
import random,json
from new_api_cases.get_statementId import statementId, statementId_no_dataset, get_sql_analyse_statement_id, get_sql_analyse_dataset_info, get_sql_execte_statement_id, steps_sql_parseinit_statemenId, steps_sql_analyzeinit_statementId
from new_api_cases.prepare_datas_for_cases import get_job_tasks_id,collector_schema_sync,get_flow_id,get_applicationId


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
        api_name = case_table_sheet.cell(row=i, column=1).value
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            if api_name == 'tenants':  # 租户的用例需要使用root用户登录后操作
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, column=8, url=request_url, headers=get_headers_root(),
                                              data=request_data, table_sheet_name=case_table_sheet)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, headers=get_headers_root(), data=request_data,
                                             table_sheet_name=case_table_sheet, row=i, column=8)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, row=i, data=request_data,
                                             table_sheet_name=case_table_sheet, column=8, headers=get_headers_root())

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(request_url, request_data, table_sheet_name=case_table_sheet, row=i,
                                                column=8, headers=get_headers_root())
                else:
                    print('请求方法%s不在处理范围内' % request_method)
            else:
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
                    put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers())

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(request_url,request_data,table_sheet_name=case_table_sheet,row=i,column=8, headers=get_headers())

                else:
                    print('请求方法%s不在处理范围内' % request_method)
        else:
            print('第 %d 行请求方法为空' % i)
    #  执行结束后保存表格
    case_table.save(ab_dir("api_cases.xlsx"))



# POST请求
def post_request_result_check(row, column, url, headers, data, table_sheet_name):
    if isinstance(data, str):
        case_detail = case_table_sheet.cell(row=row, column=2).value
        # if case_detail =='HDFS，根据statementId取结果数据(datasetId不存在)':
        if case_detail in ('HDFS，根据statementId取结果数据(datasetId不存在)', 'HIVE，根据statementId取Dataset数据(datasetId不存在)',
                           'KAFKA，根据statementId取Dataset数据(datasetId不存在)',
                           'FTP，根据statementId取Dataset数据(datasetId不存在)'):
            # 先获取statementId,然后格式化URL，再发送请求
            statement = statementId_no_dataset(dict_res(data))
            new_url = url.format(statement)
            response = requests.post(url=new_url, headers=headers, data=data)
            print(response.url)
            # 将返回的status_code和response.text分别写入第10列和第14列
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取SQL执行任务结果':
            # 先获取接口需要使用的statement id 和 数据集分析字段
            execte_statement_id = get_sql_execte_statement_id(data)  # statement id
            new_url = url.format(execte_statement_id)
            print('获取SQL执行任务结果URL:', new_url)
            execte_use_params = get_sql_analyse_dataset_info(data)  # 数据集分析字段
            # print(execte_use_params)
            response = requests.post(url=new_url, headers=headers, json=execte_use_params)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            # print(response.status_code)
            # print(response.text)
        elif case_detail == '批量删除execution':
            # 需要先查询指定flow下的所有execution，从中取出execution id，拼装成list，传递给删除接口
            query_execution_url = '%s/api/executions/query' % HOST_189
            all_exectuions = requests.post(url=query_execution_url, headers=headers, data=data)
            executions_dict = dict_res(all_exectuions.text)
            executions_content = executions_dict['content']
            try:
                all_ids = [] # 该list用来存储所有的execution id
                for item in executions_content:
                    executions_content_id = item['id']
                    all_ids.append(executions_content_id)
            except Exception as e:
                print(e)
            else:  # 取出一个id放入一个新的list，作为传递给removeLIst接口的参数
                removelist_data = []
                removelist_data.append(all_ids[-1])
                # 执行删除操作
                removeList_result = requests.post(url=url,headers=headers, json=removelist_data)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=removeList_result.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=removeList_result.text)
        elif case_detail == '停止一个采集器任务的执行':
            task_id = get_job_tasks_id(data)
            response = requests.post(url=url, headers=headers, json=task_id)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '指定目录下创建子目录':
            response = requests.post(url=url, headers=headers, json=dict_res(data))
            print(response.text)
            print(response.status_code)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        else:
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
                        # print(response.text, type(response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    print('第%d行参数查询无结果' % row)
            # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
            elif data.startswith('{') and data.endswith('}'):
                # print('data startswith {:', data)
                data_dict = dict_res(data)
                # print(data_dict)
                response = requests.post(url=url, headers=headers, json=data_dict)
                clean_vaule(table_sheet_name, row, column)
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
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    # case_table.save(ab_dir("api_cases.xlsx"))
                else:
                    print('请先确认第%d行list参数值' % row)
            else:
                    print('第%d行参数不是以startswith或者{,[开头，请先确认参数内容' % row)
    else:
        print('请确认第%d行的data形式' % row)


# GET请求
def get_request_result_check(url, headers, data, table_sheet_name, row, column):
    case_detail = case_table_sheet.cell(row=row, column=2).value

    # GET请求需要从parameter中获取参数,并把参数拼装到URL中，
    if data:
        if case_table_sheet.cell(row=row, column=2).value == '根据statement id,获取预览Dataset的结果数据(datasetId存在)':
            print(data)
            statement_id = statementId(data)
            parameter_list = []
            parameter_list.append(data)
            parameter_list.append(statement_id)
            url_new = url.format(parameter_list[0], parameter_list[1])
            response = requests.get(url=url_new, headers=headers)
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=url_new, headers=headers)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据statement id,获取Sql Analyze结果(获取输出字段)'):
            # print('888888888888888888888888888888')
            sql_analyse_statement_id = get_sql_analyse_statement_id(data)
            new_url = url.format(sql_analyse_statement_id)
            print(new_url)
            response = requests.get(url=new_url, headers=headers)
            print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束指定statementId对应的查询任务'):  # 取消SQL analyse接口
            cancel_statement_id = get_sql_analyse_statement_id(data)
            new_url = url.format(cancel_statement_id)
            response = requests.get(url=new_url, headers=headers)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        elif case_detail == ('根据解析sql parse接口返回的statementId,获取dataset name'):
            datasetName_statementId = steps_sql_parseinit_statemenId(data)
            new_url = url.format(datasetName_statementId)
            response = requests.get(url=new_url, headers=headers)
            print(response.text)
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=new_url, headers=headers)
            print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据Sql Analyze返回的statementId,获取SqlAnalyze结果'):
            steps_sql_analyse_statementId = steps_sql_analyzeinit_statementId(data)
            new_url = url.format(steps_sql_analyse_statementId)
            response = requests.get(url=new_url, headers=headers)
            print(response.text)
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=new_url, headers=headers)
            print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束sqlsource step中指定statementId对应任务'):
            cancel_sql_parseinit_statementId = steps_sql_parseinit_statemenId(data)
            new_url = url.format(cancel_sql_parseinit_statementId)
            response = requests.get(url=new_url, headers=headers)
            print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据execution id查找execution':
            # 需要先查询指定flow下的所有execution，从中取出第一个execution id，传递给查询接口
            query_execution_url = '%s/api/executions/query' % HOST_189
            all_executions = requests.post(url=query_execution_url, headers=headers, data=data)
            executions_dict = dict_res(all_executions.text)
            executions_content = executions_dict['content']
            try:
                all_ids = []  # 该list用来存储所有的execution id
                for item in executions_content:
                    executions_content_id = item['id']
                    all_ids.append(executions_content_id)
            except Exception as e:
                print(e)
            else:  #
                # 执行查询操作,将查询到的第一个execution id当做参数传递给查询接口
                new_url = url.format(all_ids[0])
                query_response = requests.get(url=new_url, headers=headers)
                print(query_response.status_code)
                print(query_response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=query_response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=query_response.text)
        elif case_detail == '查看元数据同步任务的日志进度':
            task_id = collector_schema_sync(data)
            print(task_id)
            time.sleep(5)
            new_url = url.format(task_id)
            # time.sleep(2)
            response = requests.get(url=new_url, headers=headers)
            print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        elif case_detail == '拉取元数据同步任务的日志':
            task_id = collector_schema_sync(data)
            print(task_id)
            time.sleep(5)
            new_url = url.format(task_id)
            response = requests.get(url=new_url, headers=headers)
            print(response.url)
            print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据tasks id 查看完整log':
            task_id = collector_schema_sync(data)
            print(task_id)
            time.sleep(5)
            new_url = url.format(task_id)
            response = requests.get(url=new_url, headers=headers)
            print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        elif case_detail == '导出flow':
            token = get_auth_token()
            new_url = url.format(token)
            print(token)
            response = requests.get(url=new_url,headers=get_headers())
            print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        else:
            # 分割参数，分割后成为一个列表['61bf20da-f42c-4b35-9142-0fc2a7664e3e', '2']
            parameters = data.split('&')
            print('parameters:', parameters)
            # 处理存在select语句中的参数，并重新赋值
            for i in range(len(parameters)):
                if parameters[i].startswith('select id from'):
                    select_result = ms.ExecuQuery(parameters[i])
                    parameters[i] = select_result[0]["id"]
                elif parameters[i].startswith('select name from'):
                    select_result = ms.ExecuQuery(parameters[i])
                    parameters[i] = select_result[0]["name"]
                elif parameters[i].startswith('select execution_id from'):
                    select_result = ms.ExecuQuery(parameters[i])
                    parameters[i] = select_result[0]["execution_id"]

            # 判断URL中需要的参数个数，并比较和data中的参数个数是否相等
            if len(parameters) == 1:
                url_new = url.format(parameters[0])
                response = requests.get(url=url_new, headers=headers)
                print(response.url,response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif len(parameters) == 2:
                url_new = url.format(parameters[0], parameters[1])
                response = requests.get(url=url_new, headers=headers)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif len(parameters) == 3:
                url_new = url.format(parameters[0], parameters[1], parameters[2])
                response = requests.get(url=url_new, headers=headers)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                print('请确认第%d行parameters' % row)
    # GET 请求参数写在URL中，直接发送请求
    else:
        if case_detail in('根据applicationId获取yarnAppliction任务运行状态','根据applicationId获取yarnAppliction任务的日志command line log'):
            print(case_detail)
            application_id = get_applicationId()
            new_url = url.format(application_id)
            response = requests.get(url=new_url, headers=get_headers())
            print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            response = requests.get(url=url, headers=headers)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)


# PUT请求
def put_request_result_check(url, row, data, table_sheet_name, column,headers):
    if data and isinstance(data, str):
        if '&' in data:
            # 分隔参数
            parameters = data.split('&')
            # 拼接URL
            new_url = url.format(parameters[0])
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
                print(new_data, type(new_data))
                new_url = url.format(new_data)
                print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('{') and data.endswith('}'):
                response = requests.put(url=url, headers=headers, data=data)
                print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('[') and data.endswith(']'):
                pass
            else:
                new_url = url.format(data)
                print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
    else:
        print('第%s行的参数为空或格式异常' % row)


def delete_request_result_check(url, data, table_sheet_name, row, column, headers):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if isinstance(data, str):
        if case_detail == '':
            pass
        else:
            if data.startswith('select id'):  # sql语句的查询结果当做参数
                data_select_result = ms.ExecuQuery(data)
                print(data_select_result)
                print(type(data_select_result))
                datas = []
                if data_select_result:
                    try:
                        for i in range(len(data_select_result)):
                            datas.append(data_select_result[i]["id"])
                    except:
                        print('请确认第%d行SQL语句' % row)
                    else:
                        if len(datas) == 1:
                            print(datas)
                            new_url = url.format(datas[0])
                            response = requests.delete(url=new_url, headers=headers)
                            print(response.url, response.status_code)
                            # 将返回的status_code和response.text分别写入第10列和第14列
                            clean_vaule(table_sheet_name, row, column)
                            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                        else:
                            print('请确认 select 语句查询返回值是不是只有一个')
                else:
                    print('第%d行参数查询无结果' % row)
                # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}

            else:
                new_url = url.format(data)
                response = requests.delete(url=new_url, headers=headers)
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    else:
        print(data)
        print(type(data))
        print('请确认第%d行的data形式' % row)


#  写入返回结果
def write_result(sheet, row, column, value):
    sheet.cell(row=row, column=column, value=value)


#  写入结果前，先把结果和对比结果全部清空
def clean_vaule(sheet, row, column):
    sheet.cell(row=row, column=column, value='')
    sheet.cell(row=row, column=column+1, value='')
    sheet.cell(row=row, column=column + 4, value='')
    sheet.cell(row=row, column=column + 5, value='')
    sheet.cell(row=row, column=column + 6, value='')
    sheet.cell(row=row, column=column + 7, value='')

def deal_parameters(data):
    if data:
        if '随机数' in data:
            # print(data)
            new_data = data.replace('随机数', str(random.randint(0, 999999999999999)))
            return new_data
        else:
            return data




deal_request_method()

# print(case_table_sheet.cell(row=2,column=10).value == case_table_sheet.cell(row=2,column=12).value)
# url = case_table_sheet.cell(row=2,column=7).value
# data = case_table_sheet.cell(row=2, column=8).value
# key_word = case_table_sheet.cell(row=2, column=5).value
# # post_request_result_check(key_word, row, column, url, headers, data, table_sheet_name)
# put_request_result_check( row=2, data=data, table_sheet_name=case_table_sheet, column=10)

# case_table.save(ab_dir("api_cases.xlsx"))

# print(ab_dir("api_cases.xlsx"))