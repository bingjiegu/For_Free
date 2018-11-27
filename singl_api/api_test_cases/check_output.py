from basic_info.Open_DB import MYSQL
from basic_info.data_from_db import create_schedulers
from basic_info.setting import MySQL_CONFIG
from basic_info.format_res import dict_res
import time


# 配置数据库连接
class GetCheckout(object):
    def __init__(self):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
        # 初始化schedulers
        self.new_scheduler_id = create_schedulers()
        print("初始化scheduler: %s" % self.new_scheduler_id)

    def get_execution_info(self):
        # 根据schedulers id 查询出execution id, name, 创建scheduler后查询execution有延迟，需要加等待时间
        print("开始等待40S")
        time.sleep(40)
        print('等待结束，开始执行数据库查询')
        execution_sql = 'select id, status from merce_flow_execution where flow_scheduler_id = "%s" ' % self.new_scheduler_id
        select_result = self.ms.ExecuQuery(execution_sql)
        print("根据scheduler id查询execution，id查询结果: ", select_result)
        if select_result:
            # 从查询结果中取值
            try:
                e_id = select_result[0]["id"]
                e_status = select_result[0]["status"]
            except IndexError as e:
                print("取值时报错 %s" % e )
                raise e
            else:
                # 对返回数据格式化
                e_status_format = dict_res(e_status)
                # print(e_status_format, type(e_status_format))
                e_final_status = e_status_format["type"]
                return e_id, e_final_status
        else:
            print("根据scheduler id: %s ,没有查找到execution" % self.new_scheduler_id)
            return None

    def check_out_put(self):
        # 获取execution的id和状态
        e_info = self.get_execution_info()
        e_id = e_info[0]
        e_final_status = e_info[1]
        print("开始执行check_out_put()")
        if e_id:
                while e_final_status in("READY", "RUNNING"):
                    print("进入while循环")
                    print("状态为%s" % e_final_status)
                    e_info = self.get_execution_info()
                    e_final_status = e_info[1]
                    print("此时e_final_status的状态为%s" % e_final_status)
                    # time.sleep(50)
                if e_final_status == "FAILED":
                    print("执行e_final_status == 'FAILED'语句时的状态:", e_final_status)
                    print("execution %s 执行失败" % e_id)
                    return None
                elif e_final_status == "SUCCEEDED":
                    # 成功后查询flow_execution_output表中的dataset, 即sink对应的输出dataset，取出dataset id 并返回该ID，后续调用预览接口查看数据
                    print("查询data_json_sql:")
                    data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_id
                    data_json = self.ms.ExecuQuery(data_json_sql)
                    print("状态成功时执行该语句%s" % e_final_status)
                    print("打印data_json:", data_json)
                    sink_dataset = data_json[0]["dataset_json"]  # 返回结果为元祖
                    sink_dataset_id = dict_res(sink_dataset)["id"]  # 取出json串中的dataset id
                    return sink_dataset_id
                else:
                    print("返回的状态不正确")
                    return None
        else:
            print("execution %s 不存在" % self.new_scheduler_id)


obj = GetCheckout()
sink_dataset = obj.check_out_put()
print(sink_dataset)
