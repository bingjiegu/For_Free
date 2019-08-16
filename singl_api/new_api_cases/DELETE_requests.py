import requests
from new_api_cases.clean_then_write_result import *


def delete_request_result_check(url, data, table_sheet_name, row, column, headers,case_table_sheet,ms):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if isinstance(data, str):
        if case_detail == '':
            pass
        else:
            if data.startswith('select id'):  # sql语句的查询结果当做参数
                data_select_result = ms.ExecuQuery(data)
                # print(data_select_result)
                # print(type(data_select_result))
                datas = []
                if data_select_result:
                    try:
                        for i in range(len(data_select_result)):
                            datas.append(data_select_result[i]["id"])
                    except:
                        print('请确认第%d行SQL语句' % row)
                    else:
                        if len(datas) == 1:
                            # print(datas)
                            new_url = url.format(datas[0])
                            response = requests.delete(url=new_url, headers=headers)
                            # print(response.url, response.status_code)
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
        # print(data)
        # print(type(data))
        print('请确认第%d行的data形式' % row)

