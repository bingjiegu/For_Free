import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.setting import HOST_189, tenant_id
from basic_info.format_res import dict_res

# datasetId存在时
def statementId(datasetId):
    url = '%s/api/datasets/%s/previewinit?tenant=%s' % (HOST_189, datasetId, tenant_id)
    res = requests.get(url=url, headers=get_headers())
    try:
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        return statementId
    except KeyError:
        return


# datasetId不存在时
def statementId_no_dataset(param):
    url = '%s/api/datasets/new/previewinit?tenant=%s' % (HOST_189, tenant_id)
    res = requests.post(url=url, headers=get_headers(), json=param)
    try:
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        return statementId
    except KeyError:
        return


# 初始化Sql Analyze(解析数据集输出字段)，返回statement id，获取数据集字段给分析任务使用
def get_sql_analyse_statement_id(param):
    url = ' %s/api/datasets/sql/analyzeinit' % HOST_189
    res = requests.post(url=url, headers=get_headers(), data=param)
    # print(res.text)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        # print(sql_analyse_statement_id)
        return sql_analyse_statement_id
    except KeyError:
        return


# 根据初始化SQL Analyze返回的statement id,获取数据集字段(获取输出字段)
def get_sql_analyse_dataset_info(params):
    sql_analyse_statement_id = get_sql_analyse_statement_id(params)
    # print(sql_analyse_statement_id)
    url = ' %s/api/datasets/sql/analyzeresult?statementId=%s' % (HOST_189, sql_analyse_statement_id)
    res = requests.get(url=url, headers=get_headers())
    print(res.text)
    while ("waiting") in res.text or ("running") in res.text:
        print('再次查询前',res.text)
        res = requests.get(url=url, headers=get_headers())
        print('再次查询后', res.text)
    # 返回的是str类型
    print(res.text)
    if '"statement":"available"' in res.text:
        text_dict = json.loads(res.text)
        text_dict_content = text_dict["content"]
        # print(res.text)
        # print(text_dict_content)
        return text_dict_content
    else:
        print('获取数据集输出字段失败')
        return


# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_sql_execte_statement_id(param):
    url = '%s/api/datasets/sql/executeinit' % HOST_189
    res = requests.post(url=url, headers=get_headers(), data=param)
    print(res.text)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        print(sql_analyse_statement_id)
        return sql_analyse_statement_id
    except KeyError:
        return


# 根据Sql语句解析表名,初始化ParseSql任务,返回statementID
def steps_sql_parseinit_statemenId(params):
    url = '%s/api/steps/sql/parseinit/dataflow' % HOST_189
    res = requests.post(url=url, headers=get_headers(), data=params)
    print(res.text)
    try:
        res_statementId = json.loads(res.text)
        steps_sql_parseinit_statemenId = res_statementId['statementId']
        print(steps_sql_parseinit_statemenId)
        return steps_sql_parseinit_statemenId
    except KeyError:
        return


# 初始化Sql Analyze,返回任务的statementID
def steps_sql_analyzeinit_statementId(params):
    url = '%s/api/steps/sql/analyzeinit/dataflow' % HOST_189
    res = requests.post(url=url, headers=get_headers(), data=params)
    print(res.text)
    try:
        res_statementId = json.loads(res.text)
        steps_sql_analyzeinit_statementId = res_statementId['statementId']
        print(steps_sql_analyzeinit_statementId)
        return steps_sql_analyzeinit_statementId
    except KeyError:
        return

def get_step_output_init_statementId(params):
    url = '%s/api/steps/output/fields/init' % HOST_189
    res = requests.post(url=url, headers=get_headers(), json=params)
    print(dict_res(res.text)["statementId"])
    return dict_res(res.text)["statementId"]

def get_step_output_ensure_statementId(params):
    url = '%s/api/steps/validateinit/dataflow' % HOST_189
    res = requests.post(url=url, headers=get_headers(), data=params)
    print(dict_res(res.text)["statementId"])
    return dict_res(res.text)["statementId"]

# params = '{"id":"source_9","name":"source_9","type":"source","x":168,"y":239,"otherConfigurations":{"schema":"schema_for_students_startjoin_step","schemaId":"31caabd3-ed37-415d-bc51-5c039f5b7689","sessionCache":"","datasetId":"5ebd5da6-793d-4cf9-bb4a-f84301eb0c4e","interceptor":"","dataset":"gbj_use_students_short_84","ignoreMissingPath":false},"outputConfigurations":[{"id":"output","fields":[{"column":"sId","alias":""},{"column":"sName","alias":""},{"column":"sex","alias":""},{"column":"age","alias":""},{"column":"class","alias":""}]}]}'
# get_step_output_ensure_statementId(params)




