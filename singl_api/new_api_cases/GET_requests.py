import time
from new_api_cases.get_statementId import *
from new_api_cases.clean_then_write_result import clean_vaule, write_result
from new_api_cases.prepare_datas_for_cases import collector_schema_sync, get_woven_qaoutput_dataset_path,get_applicationId
from basic_info.get_auth_token import get_auth_token

# GET请求
def get_request_result_check(case_table_sheet,url, headers, host, data, row, column,ms):
    case_detail = case_table_sheet.cell(row=row, column=2).value

    # GET请求需要从parameter中获取参数,并把参数拼装到URL中，
    if data:
        if case_detail in('预览DB dataset,获取预览Dataset的数据(Id存在)','预览dataset-HDFS-csv,获取预览Dataset的数据(Id存在)',
                          '预览dataset-HDFS-parquet,获取预览Dataset的数据(Id存在)','预览dataset-HDFS-orc,获取预览Dataset的数据(Id存在)',
                          '预览dataset-HDFS-txt,获取预览Dataset的数据(Id存在)','预览dataset-HDFS-avro,获取预览Dataset的数据(Id存在)'):
            # print(data)
            print('开始执行：', case_detail)
            # data = deal_parameters(data)
            statement_id = statementId(host, data)
            parameter_list = []
            parameter_list.append(data)
            parameter_list.append(statement_id)
            # print(parameter_list)
            url_new = url.format(parameter_list[0], parameter_list[1])
            # print(url_new)
            response = requests.get(url=url_new, headers=headers)
            # print(response.status_code, response.text)
            count_num = 0
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=url_new, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据statement id,获取Sql Analyze结果(获取输出字段)'):
            print('开始执行：', case_detail)
            sql_analyse_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(sql_analyse_statement_id)
            # print(new_url)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束指定statementId对应的查询任务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            cancel_statement_id = get_sql_analyse_statement_id(HOST_189,data)
            new_url = url.format(cancel_statement_id)
            response = requests.get(url=new_url, headers=headers)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)

        elif case_detail == ('根据解析sql parse接口返回的statementId,获取dataset name'):
            print('开始执行：', case_detail)
            datasetName_statementId = steps_sql_parseinit_statemenId(HOST_189,data)
            new_url = url.format(datasetName_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            count_num = 0
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据Sql Analyze返回的statementId,获取SqlAnalyze结果'):
            print('开始执行：', case_detail)
            steps_sql_analyse_statementId = steps_sql_analyzeinit_statementId(HOST_189, data)
            new_url = url.format(steps_sql_analyse_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            count_num = 0
            while "waiting" in response.text or "running"in response.text:
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束sqlsource step中指定statementId对应任务'):
            print('开始执行：', case_detail)
            cancel_sql_parseinit_statementId = steps_sql_parseinit_statemenId(HOST_189, data)
            new_url = url.format(cancel_sql_parseinit_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail in ('查看元数据同步任务的日志进度','拉取元数据同步任务的日志','根据tasks id 查看完整log'):
            print('开始执行：', case_detail)
            time.sleep(10)
            task_id = collector_schema_sync(data)
            # print(task_id)
            # print(task_id)
            time.sleep(5)
            new_url = url.format(task_id)
            # time.sleep(2)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '导出flow':
            print('开始执行：', case_detail)
            token = get_auth_token(host)
            new_url = url.format(token)
            # print(token)
            response = requests.get(url=new_url,headers=headers)
            # print(response.url, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statementID获取step的输出字段':
            print('开始执行：', case_detail)
            init_statementId = get_step_output_init_statementId(HOST_189, data)
            # print(init_statementId)
            new_url = url.format(init_statementId)
            response = requests.get(url=new_url, headers=headers)
            count_num = 1
            while "running" in response.text or "waiting" in response.text:
                time.sleep(5)
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.url, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statementID确认step':
            print('开始执行：', case_detail)
            ensure_statementId = get_step_output_ensure_statementId(HOST_189,data)
            # print(ensure_statementId)
            new_url = url.format(ensure_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url)
            # print(response.status_code,response.text)
            count_num = 0
            while "running" in response.text or "waiting" in response.text:
                time.sleep(5)
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.url, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)

        else:
            print('开始执行：', case_detail)
            # 分割参数，分割后成为一个列表['61bf20da-f42c-4b35-9142-0fc2a7664e3e', '2']
            parameters = data.split('&')
            # print('parameters:', parameters)
            # 处理存在select语句中的参数，并重新赋值
            for i in range(len(parameters)):
                if parameters[i].startswith('select id from'):
                    # select_result = ms.ExecuQuery(parameters[i])
                    try:
                        select_result = ms.ExecuQuery(parameters[i])
                        parameters[i] = select_result[0]["id"]
                    except:
                        print('第%s行参数没有返回结果' % row)

                elif parameters[i].startswith('select name from'):
                    try:
                        select_result = ms.ExecuQuery(parameters[i])
                        parameters[i] = select_result[0]["name"]
                    except:
                        print('第%s行参数没有返回结果' % row)
                elif parameters[i].startswith('select execution_id from'):
                    try:
                        select_result = ms.ExecuQuery(parameters[i])
                        parameters[i] = select_result[0]["execution_id"]
                    except:
                        print('第%s行参数没有返回结果' % row)
            # 判断URL中需要的参数个数，并比较和data中的参数个数是否相等
            if len(parameters) == 1:
                try:
                    url_new = url.format(parameters[0])
                    # print(url_new)
                    response = requests.get(url=url_new, headers=headers)
                    # print(response.content, response.status_code, response.text)
                except Exception:
                    return
                # print(response.url, response.status_code,response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            elif len(parameters) == 2:
                url_new = url.format(parameters[0], parameters[1])
                # print(url_new)
                response = requests.get(url=url_new, headers=headers)
                # print(response.url, response.status_code, response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            elif len(parameters) == 3:
                url_new = url.format(parameters[0], parameters[1], parameters[2])
                response = requests.get(url=url_new, headers=headers)
                # print(response.url, response.status_code, response.text)
                clean_vaule(case_table_sheet, row, column)
                write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
                write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
            else:
                print('请确认第%d行parameters' % row)
    # GET 请求参数写在URL中，直接发送请求
    else:
        if case_detail in('根据applicationId获取yarnAppliction任务运行状态','根据applicationId获取yarnAppliction任务的日志command line log'):
            print('开始执行：', case_detail)
            application_id = get_applicationId()
            new_url = url.format(application_id)
            response = requests.get(url=new_url, headers=headers)
            # print(response.status_code, response.text, type(response.text))
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据质量分析结果path预览dataset-获取datasetId':
            print('开始执行：', case_detail)
            dataset_path = get_woven_qaoutput_dataset_path()[0]
            new_url = url.format(dataset_path)
            # print(new_url)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url)
            # print(response.status_code, response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)
        else:
            print('开始执行：', case_detail)
            response = requests.get(url=url, headers=headers)
            # print(response.url)
            # print(response.content)
            # print(response.status_code,response.text)
            clean_vaule(case_table_sheet, row, column)
            write_result(sheet=case_table_sheet, row=row, column=column, value=response.status_code)
            write_result(sheet=case_table_sheet, row=row, column=column + 4, value=response.text)

