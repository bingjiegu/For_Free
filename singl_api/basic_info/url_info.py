from basic_info.setting import *

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


# ----------------login-------------------------
login_url = "%s/api/auth/login" % (MY_LOGIN_INFO["HOST"])
# ----------------dataset---------------------------
priview_url = "%s/api/datasets/%s/preview?rows=50&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (MY_LOGIN_INFO["HOST"],dataset_id)
create_dataset_url = '%s/api/datasets' % MY_LOGIN_INFO["HOST"]
# ----------------flow-------------------------------
create_flow_url = "%s/api/flows/create" % MY_LOGIN_INFO["HOST"]
# ----------------schema-----------------------
create_schema_url = '%s/api/schemas' % MY_LOGIN_INFO["HOST"]

# ----------------collector-------------------------
collector_table_url = '%s/api/woven/collectors/c1/resource/2b5ff16f-ca1b-465e-8a6d-69b8b39f8d61/tables?' % MY_LOGIN_INFO["HOST"]



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
# 查询需要更新的流程的flowid的url added by pengyuan 1120
flow_update_flowid_url = '%s/api/flows/%s/findFlow' % (MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据老的版本查询历史流程
query_flow_history_version_url = '%s/api/flows/history/%s/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id, query_flow_version)
# 根据老的id查询历史流程
query_flow_history_id_url = '%s/api/flows/history/list/%s' %(MY_LOGIN_INFO["HOST"], flow_update_id)
# 根据流程id和计划id查询执行历史
query_flow_flowAscheduler_id_url = '%s/api/flows/%s/schedulers/%s/executions' % (MY_LOGIN_INFO["HOST"], flow_update_id, flow_scheduler_id)
# 根据老的版本查询流程
query_flow_version_url = '%s/api/flows/%s/%s' % (MY_LOGIN_INFO["HOST"], flow_update_id, query_flow_version)


#------质量分析接口------
create_analysis_model = "%s/api/woven/zmod" % (MY_LOGIN_INFO["HOST"])
# query_zmod_rule = "%s/api/woven/zmodrules/%s/detailslist" % (MY_LOGIN_INFO["HOST"], zmod_id)
zmod_removeList_url = "%s/api/woven/zmod/removeList" % (MY_LOGIN_INFO["HOST"])