# coding:utf-8
import requests
from basic_info.get_auth_token import get_headers, get_headers_root,get_auth_token
from util.format_res import dict_res, get_time
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.setting import HOST_189
from new_api_cases.deal_parameters import deal_parameters
import random, unittest
from new_api_cases.get_statementId import statementId, statementId_no_dataset, get_sql_analyse_statement_id, \
    get_sql_analyse_dataset_info, get_sql_execte_statement_id, steps_sql_parseinit_statemenId, \
    steps_sql_analyzeinit_statementId,get_step_output_init_statementId,get_step_output_ensure_statementId
from new_api_cases.prepare_datas_for_cases import get_job_tasks_id,collector_schema_sync, get_applicationId,\
    get_woven_qaoutput_dataset_path,upload_jar_file_workflow,upload_jar_file_dataflow,upload_jar_file_filter
from new_api_cases.clean_then_write_result import *
# from new_api_cases.execute_cases import jar_dir

# POST请求
def post_request_result_check(row, column, url, host, headers, data, case_table_sheet,ms,jar_dir):
    if isinstance(data, str):
        case_detail = case_table_sheet.cell(row=row, column=2).value
        if case_detail in ('预览dataset-HDFS-csv,获取预览Dataset的数据(Id不存在)', '预览dataset-HDFS-parquet,获取预览Dataset的数据(Id不存在)',
                           '预览dataset-HDFS-orc,获取预览Dataset的数据(Id不存在)','预览dataset-HDFS-txt,获取预览Dataset的数据(Id不存在)',
                           '预览dataset-HDFS-avro,获取预览Dataset的数据(Id不存在)','预览dataset-DB,获取预览数据(Id不存在)'):
            # 先获取statementId,然后格式化URL，再发送请求
            print('开始执行：', case_detail)
            statement = statementId_no_dataset(host, dict_res(data))
            new_url = url.format(statement)
            data = data.encode('utf-8')
            response = requests.post(url=new_url, headers=headers, data=data)
            print(response.text, response.status_code)
            # print(response.url)
            # 将返回的status_code和response.text分别写入第10列和第14列
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)

        elif case_detail == '获取SQL执行任务结果':
            print('开始执行：', case_detail)
            # 先获取接口需要使用的statement id 和 数据集分析字段
            execte_statement_id = get_sql_execte_statement_id(HOST_189,data)  # statement id
            new_url = url.format(execte_statement_id)
            # print('获取SQL执行任务结果URL:', new_url)
            execte_use_params = get_sql_analyse_dataset_info(HOST_189,data)  # 数据集分析字段
            # print(execte_use_params)
            response = requests.post(url=new_url, headers=headers, json=execte_use_params)
            count_num = 0
            while ("waiting") in response.text or ("running") in response.text:
                # print('再次查询前',res.text)
                response = requests.post(url=new_url, headers=headers, json=execte_use_params)
                count_num += 1
                if count_num == 100:
                    return
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            # print(response.status_code)
            # print(response.text)
        elif case_detail == '批量删除execution':
            print('开始执行：', case_detail)
            # 需要先查询指定flow下的所有execution，从中取出execution id，拼装成list，传递给删除接口
            query_execution_url = '%s/api/executions/query' % HOST_189
            all_exectuions = requests.post(url=query_execution_url, headers=headers, data=data)
            executions_dict = dict_res(all_exectuions.text)
            # print(executions_dict)
            try:
                executions_content = executions_dict['content']
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
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=removeList_result.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=removeList_result.text)
        elif case_detail == '停止一个采集器任务的执行':
            print('开始执行：', case_detail)
            task_id = get_job_tasks_id(data)
            response = requests.post(url=url, headers=headers, json=task_id)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '指定目录下创建子目录':
            print('开始执行：', case_detail)
            response = requests.post(url=url, headers=headers, json=dict_res(data))
            # print(response.text)
            # print(response.status_code)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '数据源状态监控分析图数据':
            data = {"fieldList":[{"fieldName":"createTime","fieldValue":get_time(),"comparatorOperator":"GREATER_THAN","logicalOperator":"AND"},{"fieldName":"createTime","fieldValue":1555516800000,"comparatorOperator":"LESS_THAN"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8,"groupBy":"testTime"}
            response = requests.post(url=url,headers=headers,json=data)
            # print(response.status_code,response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail in ('配置工作流选择器-上传jar包', '配置过滤器-上传jar包', '配置批处理选择器-上传jar包'):
            files = {"file": open(jar_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, files=files, headers=headers)
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '注册工作流选择器':
            fileName = upload_jar_file_workflow()
            new_url = url.format(fileName)
            # print(new_url)
            # print(data)
            response = requests.post(url=new_url, headers=headers, data=data)
            # print(response.text)
            # print(response.content)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '注册批处理选择器':
            fileName = upload_jar_file_dataflow()
            new_url = url.format(fileName)
            response = requests.post(url=new_url, headers=headers, data=data)
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '注册过滤器':
            fileName = upload_jar_file_filter()
            new_url = url.format(fileName)
            response = requests.post(url=new_url, headers=headers, data=data)
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '创建name为dudu666666的用户':
            user_search = 'select id from merce_user where name = "dudu666666"'
            user_search_result = ms.ExecuQuery(user_search)
            # 先判断dudu666666用户是否存在，若存在，先执行删除操作，再创建
            if user_search_result:
                print('存在dudu666666用户，先删除再创建')
                user_id_list = []
                user_id = user_search_result[0]["id"]
                user_id_list.append(user_id)
                # print(user_id_list)
                disable_user_url = '%s/api/woven/users/disable' % HOST_189
                remove_user_url = '%s/api/woven/users/removeList' % HOST_189
                disable_user_url_dam = '%s/api/users/disable' % HOST_189
                remove_user_url_dam = '%s/api/users/removeList' % HOST_189
                # 先停用该用户
                if '57' in HOST_189:
                    res = requests.post(url=disable_user_url_dam, headers=headers, json=user_id_list)
                # print(headers)
                #     print(res.content, res.status_code, res.status_code)
                # 删除该用户
                    res2 = requests.post(url=remove_user_url_dam, headers=headers, json=user_id_list)
                    # print(res2.status_code, res2.text)
                # 创建dudu666666用户
                    response = requests.post(url=url, headers=headers, data=data)
                    # print(response.status_code, response.text)
                else:
                    res = requests.post(url=disable_user_url, headers=headers, json=user_id_list)

                # 删除该用户
                    res2 = requests.post(url=remove_user_url, headers=headers, json=user_id_list)

                # 创建dudu666666用户
                    response = requests.post(url=url, headers=headers, data=data)
                    # print(response.status_code, response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            else:
                print('不存在dudu666666用户，开始执行创建用户的用例')
                # 创建dudu666666用户
                response = requests.post(url=url, headers=headers, data=data)
                # print(response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '数据标准导入文件':
            files = {'file': open('gender.xls', 'rb')}
            # headers = get_headers()
            headers.pop('Content-Type')
            response = requests.post(url, files=files, headers=headers, params=dict_res(data))
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == "登录":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers.pop('X-AUTH-TOKEN')
            response = requests.post(url, headers=headers, data=dict_res(data))
            # print(type(data))
            # print('headers', headers)
            # print(response.content)
            # print(response.headers)
            # print(response.status_code,response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        else:
            print('开始执行：', case_detail)
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
                        clean_vaule(case_table_sheet, row, column)
                        write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                        write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
                else:
                    print('第%d行参数查询无结果' % row)
            # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
            elif data.startswith('{') and data.endswith('}'):
                # print('data startswith {:', data)
                data_dict = dict_res(data)
                # print(data_dict)
                response = requests.post(url=url, headers=headers, json=data_dict)
                # print(response.url)
                # print(response.content)
                # print(response.status_code, response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            # 列表作为参数， 如["9d3639f0-02bc-44cd-ac71-9a6d0f572632"]
            elif data.startswith('[') and data.endswith(']'):
                # print('data startswith [:', data)
                data_list = dict_res(data)
                # print(type(data_list))
                if data:
                    response = requests.post(url=url, headers=headers, json=data_list)
                    # print(response.status_code)
                    clean_vaule(case_table_sheet, row, column)
                    write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                    write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
                    # case_table.save(ab_dir("api_cases_83.xlsx"))
                else:
                    print('请先确认第%d行list参数值' % row)
            else:
                print('请确认第%d行的data形式' % row)

    else:
        print('请确认第%d行的data形式' % row)

print('post')