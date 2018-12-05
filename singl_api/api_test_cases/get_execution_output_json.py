from basic_info.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers
from basic_info.setting import MySQL_CONFIG, flow_id_list
from basic_info.format_res import dict_res, get_time
from basic_info.get_auth_token import MY_LOGIN_INFO
import time, random, requests, xlrd
from xlutils.copy import copy


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
        flow_table = xlrd.open_workbook("./api_test_cases/flow_dataset_info.xls")
        info_sheet = flow_table.sheet_by_name("flow_info")
        info_sheet_row = info_sheet.nrows
        # print(info_sheet_row)
        for i in range(0, info_sheet_row-1):
            flow_id = info_sheet.cell(i+1, 1).value
            # print(i, flow_id)
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
                    # print(flow_name, flow_type)
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
                sink_dataset_dict["execution_id"] = e_id

                sink_dataset_dict["flow_scheduler_id"] = e_scheduler_id
                if e_id:
                    print("------开始对%s 进行状态的判断------\n" % e_id)
                    while e_final_status in ("READY", "RUNNING"):
                        print("------进入while循环------\n")
                        # 状态为 ready 或者 RUNNING时，再次查询e_final_status            #
                        print("------开始等待20S------\n")
                        time.sleep(20)
                        # 调用get_e_finial_status(e_scheduler_id)再次查询状态
                        e_info = self.get_e_finial_status(e_scheduler_id)
                        # 对e_final_status 重新赋值
                        e_final_status = e_info["e_final_status"]
                        print("------再次查询后的e_final_status: %s------\n" %e_final_status)
                        # time.sleep(50)
                    if e_final_status == "FAILED":
                        print("execution %s 执行失败" % e_id)
                        sink_dataset_dict["e_final_status"] = e_final_status
                        sink_dataset_dict["o_dataset"] = ""
                        sink_dataset_list.append(sink_dataset_dict)
                        # continue
                    elif e_final_status == "KILLED":
                        print("execution %s 被杀死" % e_id)
                        sink_dataset_dict["e_final_status"] = e_final_status
                        sink_dataset_dict["o_dataset"] = ""
                        sink_dataset_list.append(sink_dataset_dict)
                        # continue
                    elif e_final_status == "SUCCEEDED":
                        # 成功后查询flow_execution_output表中的dataset, 即sink对应的输出dataset，取出dataset id 并返回该ID，后续调用预览接口查看数据
                        print("-----execution %s 执行状态为------\n %s" % (e_id, e_final_status))
                        # print("查询data_json_sql:")
                        sink_dataset_dict["e_final_status"] = e_final_status
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
        print('打印sink_dataset', sink_dataset)
        sink_dataset_json = []

        # 第一次打开表，将execution output dataset id通过预览接口返回的数据json串写入表，作为case执行得到的实际结果
        flow_table = xlrd.open_workbook('./api_test_cases/flow_dataset_info.xls')
        copy_table = copy(flow_table)
        copy_table_sheet = copy_table.get_sheet(0)
        flow_sheet = flow_table.sheet_by_name("flow_info")
        sheet_rows = flow_sheet.nrows  # 表的行数

        # 通过dataset预览接口取得数据的预览json串 result.text
        for i in range(0, len(sink_dataset)):
            dataset_id = sink_dataset[i]["o_dataset"]
            priview_url = "%s/api/datasets/%s/preview?rows=50&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (MY_LOGIN_INFO["HOST"], dataset_id)
            result = requests.get(url=priview_url, headers=get_headers())
            # print(result.url, '\n', result.text)
            # 如果flow_id相等，# 将output_dataset 的预览数据json串写入实际结果中
            for j in range(0, sheet_rows-1):
                if sink_dataset[i]["flow_id"] == flow_sheet.cell(j+1, 1).value:
                    # print(sink_dataset[i]["flow_id"])
                    # print(flow_sheet.cell(j+1, 1).value)
                    copy_table_sheet.write(j + 1, 3, sink_dataset[i]["execution_id"])
                    copy_table_sheet.write(j + 1, 4, sink_dataset[i]["e_final_status"])
                    copy_table_sheet.write(j+1, 6, result.text)
            copy_table.save('./api_test_cases/flow_dataset_info.xls')

        # 第二次打开表，对比实际结果和预期结果，一致标记为pass，不一致标记为fail
        table = xlrd.open_workbook('./api_test_cases/flow_dataset_info.xls')
        table_sheet = table.sheets()[0]
        copy_table = copy(table)
        copy_table_sheet = copy_table.get_sheet(0)

        c_rows = table_sheet.nrows

        # 实际结果写入表后，对比预期结果和实际结果
        for i in range(1, c_rows):
            if table_sheet.cell(i, 6).value and table_sheet.cell(i, 4).value == "SUCCEEDED":  # 实际结果存在
                if table_sheet.cell(i, 5).value == table_sheet.cell(i, 6).value:  # 实际结果和预期结果相等
                    copy_table_sheet.write(i, 7, "pass")
                    copy_table_sheet.write(i, 8, "")
                else:
                    copy_table_sheet.write(i, 7, "fail")
                    copy_table_sheet.write(i, 8, "execution: %s 预期结果和实际结果不一致 \n预期结果: %s\n实际结果: %s" % (table_sheet.cell(i, 3).value,
                                                                                            table_sheet.cell(i, 5).value, table_sheet.cell(i, 6).value))
            elif table_sheet.cell(i, 4).value == "FAILED":
                copy_table_sheet.write(i, 7, "fail")
                copy_table_sheet.write(i, 8, "execution: %s 执行状态为 %s" % (table_sheet.cell(i, 3).value, table_sheet.cell(i, 4).value ))

            copy_table.save('./api_test_cases/flow_dataset_info.xls')
        # print("表操作结束，并保存")


if __name__ == '__main__':
    # sink_dataet_json = [{'flow_id': '35033c8d-fadc-4628-abf9-6803953fba34', 'execution_id': '39954be8-900a-4466-bc2e-05e379697fef', 'flow_scheduler_id': '8cf78c22-a561-4e5b-9c1c-b709ae8a51fe', 'e_final_status': 'FAILED', 'o_dataset': ''}, {'flow_id': 'f2677db1-6923-42a1-8f18-f8674394580a', 'execution_id': 'a38d303f-5bf5-441b-831c-92df5a9b7299', 'flow_scheduler_id': '65d1ca0a-4f0d-4680-b667-291ca412bdb2', 'e_final_status': 'SUCCEEDED', 'o_dataset': 'b896ff9d-691e-4939-a860-38eb828b1ad2'}]
    # GetCheckoutDataSet()
    obj = GetCheckoutDataSet()
    # sink_dataset = obj.check_out_put()
    sink_dataet_json = obj.get_json()
    print(sink_dataet_json)


