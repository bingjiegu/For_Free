from openpyxl import load_workbook
import os
table_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def get_result():
    case_table = load_workbook(table_dir('api_cases.xlsx'))
    case_table_sheet = case_table.get_sheet_by_name('developer')
    all_rows = case_table_sheet.max_row
    for row in range(2,all_rows+1):
        # status code
        ex_stutus_code = case_table_sheet.cell(row=row, column=9).value
        ac_stutus_code = case_table_sheet.cell(row=row, column=10).value
        # text
        ex_text = case_table_sheet.cell(row=row, column=12).value
        ac_text = case_table_sheet.cell(row=row, column=13).value
        # 判断两个status code是否相等
        if ex_stutus_code == ac_stutus_code:
            print('pass')
            case_table_sheet.cell(row=row, column=11, value='pass')
        else:
            case_table_sheet.cell(row=row, column=11, value='status code预期结果和实际结果不一致，预期结果：%s, 实际结果：%s' % (ex_stutus_code, ac_stutus_code))
            print('status code预期结果和实际结果不一致，预期结果：%s, 实际结果：%s' % (ex_stutus_code, ac_stutus_code))
        # 判断两个text是否相等
        if ex_text == ac_text:
            print('pass')
            case_table_sheet.cell(row=row, column=14, value='pass')
        else:
            case_table_sheet.cell(row=row, column=14, value='response.text预期结果和实际结果不一致，预期结果：%s, 实际结果：%s' % (ex_text, ac_text))
            print('response.text预期结果和实际结果不一致，预期结果：%s, 实际结果：%s' % (ex_text, ac_text))
        case_table.save(table_dir('api_cases.xlsx'))



def compair_result():
    case_table = load_workbook(table_dir('api_cases.xlsx'))
    case_table_sheet = case_table.get_sheet_by_name('developer')
    all_rows = case_table_sheet.max_row
    for row in range(2, all_rows + 1):
        status_code_result = case_table_sheet.cell(row=row, column=11).value
        response_text_result = case_table_sheet.cell(row=row, column=11).value
        if status_code_result == 'pass' and response_text_result == 'pass':
            # print('测试用例:%s 测试通过' % case_table_sheet.cell(row=row, column=3).value)
            case_table_sheet.cell(row=row, column=15, value='pass')
            case_table_sheet.cell(row=row, column=16, value='')
        elif status_code_result == 'pass' and response_text_result != 'pass':
            # print('测试用例:%s 测试未通过，错误原因:%s' % (case_table_sheet.cell(row=row, column=3).value, response_text_result ))
            case_table_sheet.cell(row=row, column=15, value='fail')
            case_table_sheet.cell(row=row, column=16, value='错误原因:%s' % response_text_result)
        elif status_code_result != 'pass' and response_text_result == 'pass':
            # print('测试用例:%s 测试未通过，错误原因:%s' % (case_table_sheet.cell(row=row, column=3).value, status_code_result ))
            case_table_sheet.cell(row=row, column=15, value='fail')
            case_table_sheet.cell(row=row, column=16, value='错误原因:%s' % status_code_result)
        elif status_code_result != 'pass' and response_text_result != 'pass':
            # print('测试用例:%s 测试未通过，错误原因:%s\n %s' % (case_table_sheet.cell(row=row, column=3).value, status_code_result, response_text_result))
            case_table_sheet.cell(row=row, column=15, value='fail')
            case_table_sheet.cell(row=row, column=16, value='错误原因:%s\n %s' % (status_code_result, response_text_result))
        else:
            print('请确认接口返回的response')

        case_table.save(table_dir('api_cases.xlsx'))
compair_result()