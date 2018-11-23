import pymysql
import requests
import json
import logging

from basic_info.Open_DB import MYSQL
from basic_info.data_from_db import create_schedulers, get_flows
from basic_info.setting import MySQL_CONFIG, flow_id
from basic_info.get_auth_token import get_headers
from basic_info.format_res import dict_res
import time

# 配置数据库连接
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])


class GetCheckout(object):
    def __init__(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
        # 初始化schedulers
        # logging.debug("debug logging")
        self.new_scheduler_id = create_schedulers()
        print('初始化scheduler id：', self.new_scheduler_id)

    def get_execution_info(self):
        # 根据schedulers id 查询出execution id, name, 创建scheduler后查询execution有延迟，需要加等待时间
        print("开始等待40S")
        time.sleep(40)
        print('等待结束，开始执行数据库查询')
        # logging.debug("debug logging")
        execution_sql = 'select id, status from merce_flow_execution where flow_scheduler_id = "%s" ' % self.new_scheduler_id
        result = self.ms.ExecuQuery(execution_sql)
        # print("result: %s" % result)
        if result:
            # 从查询结果中取值
            result = dict_res(result)
            print("查询结果是：", result)
            try:
                e_id = result[0][0]
                e_status = result[0][1]
            except IndexError as e:
                print("取值时报错 %s" % e )
                raise e
            else:
                e_status_format = dict_res(e_status)
                e_final_status = e_status_format["type"]
                print(e_id, e_final_status)
                return e_id, e_final_status
        else:
            print("根据scheduler id: %s ,没有查找到execution" % self.new_scheduler_id)

    def checkoutput(self):
        # print("this is checkoutput")
        # 查询execution的执行状态
        e_status_type = ["READY", "RUNNING", "SUCCESSED", "FAILED"]
        e_info = GetCheckout().get_execution_info()
        print(e_info)
        e_id = e_info[0]
        e_final_status = e_info[1]
        # print('打印 e_id, e_final_status', e_id, e_final_status)
        print("开始执行checkoutput()")
        print('e_id:', e_id, 'e_final_status', e_final_status)
        if e_id:
            print("e_id存在： %s" % e_id)
            count = 0
            for e_final_status in e_status_type:
                print("开始执行if语句")
                if e_final_status in("READY", "RUNNING"):
                    print("e_final_status == 'READY' or e_final_status == 'RUNNING'", e_final_status)
                    time.sleep(5)
                    self.get_execution_info()
                    count += 1
                    print("第%d次查询状态是%s" % (count, e_final_status))
                    continue
                elif e_final_status == "FAILED":
                    print("执行e_final_status == 'FAILED'语句时的状态:", e_final_status)
                    print("execution %s 执行失败" % e_id)
                    break
                else:
                    data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_id
                    data_json = self.ms.ExecuQuery(data_json_sql)
                    print("最终的状态：", e_final_status)
                    print("状态成功时执行该语句")
                    # 成功后查询dataset，取出dataset id ， 通过预览接口返回预览的json串
                    return data_json
        else:
            print("execution %s 不存在" % self.new_scheduler_id)

# 4. output和预期结果做比对

# 5. 删除这个schedulers


obj = GetCheckout().get_execution_info()
print(obj)
