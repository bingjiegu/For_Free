import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.setting import HOST_189, tenant_id

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
    while res.text in('{"statement":"waiting"}', '{"statement":"running"}'):
        # print('再次查询前',res.text)
        res = requests.get(url=url, headers=get_headers())
        # print('再次查询后', res.text)
    # 返回的是str类型
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
    url = 'http://192.168.1.189:8515/api/datasets/sql/executeinit'
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
    url = 'http://192.168.1.189:8515/api/steps/sql/parseinit'
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
    url = 'http://192.168.1.189:8515/api/steps/sql/analyzeinit'
    res = requests.post(url=url, headers=get_headers(), data=params)
    print(res.text)
    try:
        res_statementId = json.loads(res.text)
        steps_sql_analyzeinit_statementId = res_statementId['statementId']
        print(steps_sql_analyzeinit_statementId)
        return steps_sql_analyzeinit_statementId
    except KeyError:
        return





# params = {"datasets":"gbj_mysql_datasource_189_dataset","sql":"select * from gbj_mysql_datasource_189_dataset"}
# steps_sql_analyzeinit_statementId(params)





