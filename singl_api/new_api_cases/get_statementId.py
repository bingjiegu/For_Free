import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.setting import HOST_189, tenant_id

# datasetId存在时
def statementId(datasetId):
    url = '%s/api/datasets/%s/previewinit?tenant=%s' % (HOST_189, datasetId, tenant_id)
    res = requests.get(url=url, headers=get_headers())
    res_statement = json.loads(res.text)
    statementId = res_statement['statementId']

    return statementId

# datasetId不存在时
def statementId_no_dataset(data):
    url = '%s/api/datasets/new/previewinit?tenant=%s' % (HOST_189, tenant_id)

    res = requests.post(url=url, headers=get_headers(), json=data)
    print(res.text)
    res_statement = json.loads(res.text)
    statementId = res_statement['statementId']
    return statementId






# print(get_statementId('1bec5f81-0fe9-4586-9033-2617028fee2a'))