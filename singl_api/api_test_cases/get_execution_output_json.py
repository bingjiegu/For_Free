from basic_info.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers
from basic_info.setting import MySQL_CONFIG, flow_id_list
from basic_info.format_res import dict_res, get_time
from basic_info.data_from_db import get_json
import time, random, requests


class GetCheckoutDataSet(object):
    """该类用来获取批量创建的scheduler对应的execution，执行成功后sink所输出的 dataset id"""
    def __init__(self):
        """初始化数据库连接"""
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

    def data_for_create_scheduler(self):
        """获取setting.py中的flow_id_list
        1. 根据flow_id 查找flow_name等信息
        2. 根据查询到的flow信息，拼装创建scheduler所需要使用的data
        """
        print("------开始执行data_for_create_scheduler(self)------\n")
        data_list = []
        for i in range(len(flow_id_list)):
            flow_id = flow_id_list[i]["flow_id"]
            # print('flow_id', flow_id)
            try:
                sql = 'select name, flow_type from merce_flow where id = "%s"' % flow_id
                flow_info = self.ms.ExecuQuery(sql)
            except Exception as e:
                raise e
            else:
                try:
                    flow_name = flow_info[0]["name"]
                    flow_type = flow_info[0]["flow_type"]
                except KeyError as e:
                    raise e

            data = {
                "configurations": {
                    "arguments": [],
                    "properties": [
                        {
                            "name": "all.debug",
                            "value": "false"
                        },
                        {
                            "name": "all.dataset-nullable",
                            "value": "false"
                        },
                        {
                            "name": "all.lineage.enable",
                            "value": "true"
                        },
                        {
                            "name": "all.notify-output",
                            "value": "false"
                        },
                        {
                            "name": "all.debug-rows",
                            "value": "20"
                        },
                        {
                            "name": "dataflow.master",
                            "value": "yarn"
                        },
                        {
                            "name": "dataflow.deploy-mode",
                            "value": "client"
                        },
                        {
                            "name": "dataflow.queue",
                            "value": "a1"
                        },
                        {
                            "name": "dataflow.num-executors",
                            "value": "2"
                        },
                        {
                            "name": "dataflow.driver-memory",
                            "value": "512M"
                        },
                        {
                            "name": "dataflow.executor-memory",
                            "value": "1G"
                        },
                        {
                            "name": "dataflow.executor-cores",
                            "value": "2"
                        },
                        {
                            "name": "dataflow.verbose",
                            "value": "true"
                        },
                        {
                            "name": "dataflow.local-dirs",
                            "value": ""
                        },
                        {
                            "name": "dataflow.sink.concat-files",
                            "value": "true"
                        }
                    ],
                    "startTime": get_time()
                },
                "flowId": flow_id,
                "flowName": flow_name,
                "flowType": flow_type,
                "name": "students_flow" + str(random.randint(0, 99999)),
                "schedulerId": "once",
                "source": "rhinos"
            }

            data_list.append(data)
        # print(len(data_list))
        print("------data_for_create_scheduler(self)执行结束------\n")
        return data_list

    def create_new_scheduler(self):
        """该方法使用data_for_create_scheduler()返回的data_list批量创建scheduler，并返回scheduler_id_list"""
        print("------开始执行create_new_scheduler(self)------\n")
        from basic_info.url_info import create_scheduler_url
        scheduler_id_list = []
        for data in self.data_for_create_scheduler():
            res = requests.post(url=create_scheduler_url, headers=get_headers(), json=data)
            # print(res.status_code, res.text)
            if res.status_code == 201 and res.text:
                scheduler_id_format = dict_res(res.text)
                try:
                    scheduler_id = scheduler_id_format["id"]
                except KeyError as e:
                    print("scheduler_id_format中存在异常%s" % e)
                else:
                    scheduler_id_list.append(scheduler_id)

            else:
                return None
        print("------create_new_scheduler(self)执行结束, 返回scheduler_id_list------\n")
        return scheduler_id_list

    def get_e_finial_status(self, scheduler_id):
        """ 根据get_execution_info(self)返回的scheduler  id, 查询该scheduler的execution 状态"""
        print("------开始执行get_e_finial_status(self, scheduler_id)------\n")
        if scheduler_id:
            # 查询前先等待3S
            # time.sleep(3)
            execution_sql = 'select id, status, flow_id , flow_scheduler_id from merce_flow_execution where flow_scheduler_id = "%s" ' % scheduler_id
            select_result = self.ms.ExecuQuery(execution_sql)
            # print("根据scheduler id %s 查询execution，查询结果 %s: " % (scheduler_id, select_result))
            if select_result:
                e_info = {}
                # 从查询结果中取值
                try:
                    e_id = select_result[0]["id"]
                    print(e_id)
                    e_info["e_id"] = e_id
                    e_info["flow_id"] = select_result[0]["flow_id"]
                    e_info["flow_scheduler_id"] = select_result[0]["flow_scheduler_id"]
                    e_status = select_result[0]["status"]
                except IndexError as e:
                    print("取值时报错 %s" % e)
                    raise e
                else:
                    # 对返回数据格式化
                    e_status_format = dict_res(e_status)
                    e_final_status = e_status_format["type"]
                e_info["e_final_status"] = e_final_status  #
                # 将 execution id , flow_id和status组装成字典的形式并返回
                print("------get_e_finial_status(self, scheduler_id)执行成功，返回execution id和status------\n")
                return e_info
            else:
                # print("根据scheduler id: %s ,没有查找到execution" % scheduler_id)
                return None
        else:
            return None

    def get_execution_info(self):
        """根据schedulers id 查询出execution id, name, 创建scheduler后查询execution有延迟，需要加等待时间"""
        print("------开始执行get_execution_info(self)------\n")
        scheduler_id_list = self.create_new_scheduler()
        # scheduler_id_list = ["182a8ca9-6540-4cdc-9d6a-3e3583532067","e5c5362a-09d4-4975-b5ab-a5f0ae39c6e6"]
        if scheduler_id_list:
            e_info_list = []
            for scheduler_id in scheduler_id_list:
                # print('第 %d 个 scheduler_id %s  ' % (count, scheduler_id))
                # 等待40S后查询
                time.sleep(40)
                # print('调用get_e_finial_status(scheduler_id)，查询e_info')
                # 若没有查到execution id， 需要再次查询
                e_info = self.get_e_finial_status(scheduler_id)
                e_info_list.append(e_info)

            # print('查询得到的e_info_list', e_info_list)
            print("------get_execution_info(self)执行结束------\n")
            return e_info_list
        else:
            print("返回的scheduler_id_list为空", scheduler_id_list)
            return None

    def check_out_put(self):
        """获取execution的id和状态, 最终返回execution执行成功后的dataset id """
        print("------开始执行check_out_put(self)------\n")
        e_info_list = self.get_execution_info()
        print("------check_out_put中得到的e_info:------\n", type(e_info_list), e_info_list)
        # 返回的len(e_info)和 len(flow_id_list)相等时，数据无缺失，进行后续的判断
        if len(e_info_list) == len(flow_id_list):
            sink_dataset_list = []
            for i in range(len(e_info_list)):
                sink_dataset_dict = {}
                e_id = e_info_list[i]["e_id"]
                e_final_status = e_info_list[i]["e_final_status"]
                e_scheduler_id = e_info_list[i]["flow_scheduler_id"]
                e_flow_id = e_info_list[i]["flow_id"]
                sink_dataset_dict["flow_id"] = e_flow_id
                if e_id:
                    print("------开始对%s 进行状态的判断------\n" % e_id)
                    while e_final_status in("READY", "RUNNING"):
                        print("------进入while循环------\n")
                        # 状态为 ready 或者 RUNNING时，再次查询e_final_status            #
                        print("------开始等待20S------\n")
                        time.sleep(20)
                        # 调用get_e_finial_status(e_scheduler_id)再次查询状态
                        e_info = self.get_e_finial_status(e_scheduler_id)
                        # 对e_final_status 重新赋值
                        e_final_status = e_info["e_final_status"]
                        # time.sleep(50)
                    if e_final_status == "FAILED":
                        print("execution %s 执行失败" % e_id)
                        continue
                    elif e_final_status == "KILLED":
                        print("execution %s 被杀死" % e_id)
                        continue
                    elif e_final_status == "SUCCEEDED":
                        # 成功后查询flow_execution_output表中的dataset, 即sink对应的输出dataset，取出dataset id 并返回该ID，后续调用预览接口查看数据
                        print("-----execution %s 执行状态为------\n %s" % (e_id, e_final_status))
                        # print("查询data_json_sql:")
                        data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_id
                        data_json = self.ms.ExecuQuery(data_json_sql)
                        # print("打印data_json:", data_json)
                        sink_dataset = data_json[0]["dataset_json"]  # 返回结果为元祖
                        # print(sink_dataset)
                        sink_dataset_id = dict_res(sink_dataset)["id"]  # 取出json串中的dataset id
                        sink_dataset_dict["o_dataset"] = sink_dataset_id
                        sink_dataset_list.append(sink_dataset_dict)
                        print('第%d次的sink_dataset_list %s' % (i, sink_dataset_list))
                    else:
                        print("返回的execution 执行状态不正确")
                        return
                else:
                    print("execution不存在")
                    return
            print('最后返回的sink_dataset_list\n', sink_dataset_list)
            print("------check_out_put(self)执行结束------\n")
            # 调用get_json()方法获取dataset_json
            return sink_dataset_list
        else:
            print("返回的scheduler_id_list值缺失")
            return

    def get_json(self):
        print("------开始执行get_json()------\n")
        sink_dataset = self.check_out_put()
        from basic_info.url_info import priview_url
        sink_dataset_json = []
        for i in range(len(sink_dataset)):
            json_dict = {}
            json_dict["flow_id"] = sink_dataset[i]["flow_id"]
            dataset_id = sink_dataset[i]["o_dataset"]
            result = requests.get(url=priview_url, headers=get_headers())
            json_dict["dataset_json"] = result.json()
            sink_dataset_json.append(json_dict)
        print("------最后返回的sink_dataset_json： ------\n %s" % sink_dataset_json)
        print("------get_json()执行结束------\n")
        return sink_dataset_json



if __name__ == '__main__':
    GetCheckoutDataSet()
