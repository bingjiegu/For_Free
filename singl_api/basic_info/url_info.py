from basic_info.setting import *
from basic_info.data_from_db import get_schedulers


# 创建schedule的接口
create_scheduler_url = "%s/api/schedulers" % MY_LOGIN_INFO["HOST"]

# 查询schedule的接口
query_scheduler_url = "%s/api/schedulers/query" % (MY_LOGIN_INFO["HOST"])
select_by_schedulerId_url = "%s/api/schedulers/%s" % (MY_LOGIN_INFO["HOST"], get_schedulers())

# 启用计划接口
enable_scheduler_url = "%s/api/schedulers/enable" % (MY_LOGIN_INFO["HOST"])
# 停用计划接口
disable_scheduler_url = "%s/api/schedulers/disable" % (MY_LOGIN_INFO["HOST"])

# 查询flow接口
query_flows_url = "%s/api/flows/query" % (MY_LOGIN_INFO["HOST"])
# 创建flow接口
create_flows_url = "%s/api/flows/create" % (MY_LOGIN_INFO["HOST"])
# 根据名称查询流程接口
query_flowname_url = "%s/api/flows/flowname/%s" % (MY_LOGIN_INFO["HOST"], query_flow_name)
# 根据名称和版本查询历史流程接口
query_flowname_version_url = "%s/api/flows/name/%s/%s" % (MY_LOGIN_INFO["HOST"], query_flow_name,query_flow_version)
# 查询简化版流程
query_flow_all_url = '%s/api/flows/all' % (MY_LOGIN_INFO["HOST"])
# 更新流程
flow_update_url = '%s/api/flows/%s' % (MY_LOGIN_INFO["HOST"],flow_update_id)
# 根据老的版本查询历史流程
query_flow_history_version_url = '%s/api/flows/history/%s/%s' %(MY_LOGIN_INFO["HOST"],flow_update_id,query_flow_version)
# 根据老的id查询历史流程
query_flow_history_id_url = '%s/api/flows/history/list/%s' %(MY_LOGIN_INFO["HOST"],flow_update_id)
# 根据流程id和计划id查询执行历史
query_flow_flowAscheduler_id_url = '%s/api/flows/%s/schedulers/%s/executions' %(MY_LOGIN_INFO["HOST"],flow_update_id,flow_scheduler_id)
# 根据老的版本查询流程
query_flow_version_url = '%s/api/flows/%s/%s' %(MY_LOGIN_INFO["HOST"],flow_update_id,query_flow_version)