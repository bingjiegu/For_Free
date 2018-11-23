from basic_info.setting import *
from basic_info.get_flow_body import create_flow
from basic_info.setting import scheduler_id

# -------------------------schedulers---------------------------------------------------
# 创建scheduler的接口
create_scheduler_url = "%s/api/schedulers" % MY_LOGIN_INFO["HOST"]
# 查询scheduler的接口
query_scheduler_url = "%s/api/schedulers/query" % (MY_LOGIN_INFO["HOST"])
select_by_schedulerId_url = "%s/api/schedulers/%s" % (MY_LOGIN_INFO["HOST"], scheduler_id)
# 启用scheduler接口

enable_scheduler_url = "%s/api/schedulers/enable" % (MY_LOGIN_INFO["HOST"])
# 停用scheduler接口
disable_scheduler_url = "%s/api/schedulers/disable" % (MY_LOGIN_INFO["HOST"])
# 批量删除schedulers
remove_list_url = "%s/api/schedulers/removeList" % (MY_LOGIN_INFO["HOST"])
# 更新schedulers, scheduler_id给定
update_scheduler_url = "%s/api/schedulers/%s" % (MY_LOGIN_INFO["HOST"], scheduler_id)

# -------------------------executions----------------------------------------------------
# 查询execution
query_exectution_url = "%s/api/executions/query" % MY_LOGIN_INFO["HOST"]
# 批量查询execution
gQuery_execution_url = "%s/api/executions/groupQuery" % MY_LOGIN_INFO["HOST"]

# 批量删除schedulers
delete_schedulers_url = "%s/api/schedulers/removeList" % (MY_LOGIN_INFO["HOST"])

# 查询flow接口
query_flows_url = "%s/api/flows/query" % (MY_LOGIN_INFO["HOST"])
# 创建flow接口
create_flows_url = "%s/api/flows/create" % (MY_LOGIN_INFO["HOST"])
# 根据名称查询流程接口
query_flowname_url = "%s/api/flows/flowname/%s" % (MY_LOGIN_INFO["HOST"], query_flow_name)
# 根据名称和版本查询历史流程接口
query_flowname_version_url = "%s/api/flows/name/%s/%s" % (MY_LOGIN_INFO["HOST"], query_flow_name, query_flow_version)
# 查询简化版流程
query_flow_all_url = '%s/api/flows/all' % (MY_LOGIN_INFO["HOST"])
# 更新流程
flow_update_url = '%s/api/flows/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 查询需要更新的流程的flowid的url
flow_update_flowid_url = '%s/api/flows/%s/findFlow' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据老的版本查询历史流程
query_flow_history_version_url = '%s/api/flows/history/%s/%s' % (
    MY_LOGIN_INFO["HOST"], flow_update_id, query_flow_version)
# 根据老的id查询历史流程
query_flow_history_id_url = '%s/api/flows/history/list/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据流程id和计划id查询执行历史
query_flow_flowAscheduler_id_url = '%s/api/flows/%s/schedulers/%s/executions' % (
    MY_LOGIN_INFO["HOST"], flow_update_id, flow_scheduler_id)
# 根据老的版本查询流程
query_flow_version_url = '%s/api/flows/%s/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id, query_flow_version)
# 更新执行计划
flow_update_schedulers_url = '%s/api/flows/%s/schedulers' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据id和版本更新流程
flow_id_version_update_url = '%s/api/flows/update/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 更新版本
flow_name_version_update_url = "%s/api/flows/name/%s/%s" % (MY_LOGIN_INFO["HOST"], query_flow_name, 13)
# 根据id查询flow
flow_queryById_url = "%s/api/flows/%s/findFlow" % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据id查询版本
flow_queryById_Version_url = "%s/api/flows/%s" % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 查询运行时的一些属性
query_flow_id_runP_url = "%s/api/flows/%s/runtime-properties" % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据用户名更新流程
flow_updateByName_url = '%s/api/flows/name/%s' % (MY_LOGIN_INFO["HOST"], query_flow_name)
# 导出流程
flow_export_url = '%s/api/flows/export' % (MY_LOGIN_INFO["HOST"])
# 清理status
flow_clean_status_url = '%s/api/flows/%s/clean-saved-state' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据id删除project_flow
flow_delete_ByProjectId_url = '%s/api/flows/%s/%s' % (
    MY_LOGIN_INFO["HOST"], flow_project_id_delete, flow_project_flow_id)
# 根据id批量删除流程
flow_delete_removeList_url = '%s/api/flows/removeList' % (MY_LOGIN_INFO["HOST"])
# 根据id批量删除流程-project
flow_delete_removeListProject_url = '%s/api/flows/removeListProject/%s' % (
    MY_LOGIN_INFO["HOST"], flow_project_id_delete)
