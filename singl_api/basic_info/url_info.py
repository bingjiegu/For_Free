from basic_info.setting import MY_LOGIN_INFO
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

