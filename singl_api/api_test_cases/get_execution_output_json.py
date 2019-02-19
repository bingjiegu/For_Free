# coding=gbk
from basic_info.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers
from basic_info.setting import MySQL_CONFIG, flow_id_list
from basic_info.format_res import dict_res, get_time
from basic_info.setting import HOST_189
import time, random, requests, xlrd
from xlutils.copy import copy
from openpyxl import load_workbook
from xlutils.copy import copy
import json
import os,threading
abs_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))



class GetCheckoutDataSet(object):
    """����������ȡ����������scheduler��Ӧ��execution��ִ�гɹ���sink������� dataset id"""

    def __init__(self):
        """��ʼ�����ݿ�����"""
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

    def file_flowid_count(self):
        # ------��ȡxsl���е�flow_id��Ȼ�����װ��list�ﷵ��------
        # flow_table = xlrd.open_workbook(abs_dir("flow_dataset_info.xlsx"))
        # info_sheet = flow_table.sheet_by_name("flow_info")
        # info_sheet_row = info_sheet.nrows
        # ---ʹ��openpyxl������ 12.26update---
        flow_table = load_workbook(abs_dir("flow_dataset_info.xlsx"))
        # info_sheet_names = flow_table.get_sheet_names()
        info_sheet = flow_table.get_sheet_by_name('flow_info')
        info_sheet_row = info_sheet.max_row  # ��ȡ����
        flowid_list = []
        for i in range(2, info_sheet_row+1):
            if info_sheet.cell(row=i, column=2).value and len(info_sheet.cell(row=i, column=2).value) > 10:
                flowid_list.append(info_sheet.cell(row=i, column=2).value)
        return flowid_list


    def data_for_create_scheduler(self):
        """��ȡsetting.py�е�flow_id_list
        1. ����flow_id ����flow_name����Ϣ
        2. ���ݲ�ѯ����flow��Ϣ��ƴװ����scheduler����Ҫʹ�õ�data
        """
        print("------��ʼִ��data_for_create_scheduler(self)------\n")
        data_list = []
        # print(info_sheet_row)
        flowid_count = self.file_flowid_count()
        for flow_id in flowid_count:
            # print('------',flow_id, '--------')
            # print(i, flow_id)
            print('flow_id', flow_id)
            try:
                sql = 'select name, flow_type from merce_flow where id = "%s"' % flow_id
                print(sql)
                flow_info = self.ms.ExecuQuery(sql)
                # print('flow_info:',flow_info)
            except Exception as e:
                raise e
            else:
                try:
                    flow_name = flow_info[0]["name"]
                    print('flow_name: ', flow_name)
                    flow_type = flow_info[0]["flow_type"]
                    # print(flow_name, flow_type)
                except KeyError as e:
                    raise e
                except IndexError as T:
                    raise T
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
                            "value": "merce.normal"
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
                "name": "students_flow" + str(random.randint(0, 9999))+str(random.randint(0, 9999)),
                "schedulerId": "once",
                "source": "rhinos"
            }

            data_list.append(data)
        # print(len(data_list))
        print("------data_for_create_scheduler(self)ִ�н���------\n")
        print(len(data_list))
        return data_list

    def create_new_scheduler(self):
        """�÷���ʹ��data_for_create_scheduler()���ص�data_list��������scheduler��������scheduler_id_list"""
        print("------��ʼִ��create_new_scheduler(self)------\n")
        from basic_info.url_info import create_scheduler_url
        scheduler_id_list = []
        for data in self.data_for_create_scheduler():
            time.sleep(30)
            res = requests.post(url=create_scheduler_url, headers=get_headers(), json=data)
            print(res.status_code, res.text)
            if res.status_code == 201 and res.text:
                scheduler_id_format = dict_res(res.text)
                try:
                    scheduler_id = scheduler_id_format["id"]
                except KeyError as e:
                    print("scheduler_id_format�д����쳣%s" % e)
                else:
                    scheduler_id_list.append(scheduler_id)
            else:
                print("flow: %s scheduler����ʧ��" % data["flowid"])
                # return None
        print("------create_new_scheduler(self)ִ�н���, ����scheduler_id_list------\n")
        return scheduler_id_list

    def get_e_finial_status(self, scheduler_id):
        """ ����get_execution_info(self)���ص�scheduler  id, ��ѯ��scheduler��execution ״̬"""
        print("------��ʼִ��get_e_finial_status(self, scheduler_id)------\n")
        if scheduler_id:
            # ��ѯǰ�ȵȴ�5S
            time.sleep(5)
            execution_sql = 'select id, status, flow_id , flow_scheduler_id from merce_flow_execution where flow_scheduler_id = "%s" ' % scheduler_id
            select_result = self.ms.ExecuQuery(execution_sql)
            # print("����scheduler id %s ��ѯexecution����ѯ��� %s: " % (scheduler_id, select_result))
            if select_result:
                e_info = {}
                # �Ӳ�ѯ�����ȡֵ
                try:
                    e_id = select_result[0]["id"]
                    print(e_id)
                    e_info["e_id"] = e_id
                    e_info["flow_id"] = select_result[0]["flow_id"]
                    e_info["flow_scheduler_id"] = select_result[0]["flow_scheduler_id"]
                    e_status = select_result[0]["status"]
                except IndexError as e:
                    print("ȡֵʱ���� %s" % e)
                    raise e
                else:
                    # �Է������ݸ�ʽ��
                    e_status_format = dict_res(e_status)
                    e_final_status = e_status_format["type"]
                e_info["e_final_status"] = e_final_status  #
                # �� execution id , flow_id��status��װ���ֵ����ʽ������
                print("------get_e_finial_status(self, scheduler_id)ִ�гɹ�������execution id��status------\n")
                return e_info
            else:
                # print("����scheduler id: %s ,û�в��ҵ�execution" % scheduler_id)
                return None
        else:
            return None

    def get_execution_info(self):
        """����schedulers id ��ѯ��execution id, name, ����scheduler���ѯexecution���ӳ٣���Ҫ�ӵȴ�ʱ��"""
        print("------��ʼִ��get_execution_info(self)------\n")
        scheduler_id_list = self.create_new_scheduler()
        # scheduler_id_list = ["182a8ca9-6540-4cdc-9d6a-3e3583532067","e5c5362a-09d4-4975-b5ab-a5f0ae39c6e6"]
        if scheduler_id_list:
            e_info_list = []
            for scheduler_id in scheduler_id_list:
                # print('�� %d �� scheduler_id %s  ' % (count, scheduler_id))
                # �ȴ�40S���ѯ
                time.sleep(40)
                # print('����get_e_finial_status(scheduler_id)����ѯe_info')
                # ��û�в鵽execution id�� ��Ҫ�ٴβ�ѯ
                e_info = self.get_e_finial_status(scheduler_id)
                e_info_list.append(e_info)

            # print('��ѯ�õ���e_info_list', e_info_list)
            print("------get_execution_info(self)ִ�н���------\n")
            return e_info_list
        else:
            print("���ص�scheduler_id_listΪ��", scheduler_id_list)
            return None

    def check_out_put(self):
        """��ȡexecution��id��״̬, ���շ���executionִ�гɹ����dataset id """
        print("------��ʼִ��check_out_put(self)------\n")
        e_info_list = self.get_execution_info()
        print("------check_out_put�еõ���e_info:------\n", type(e_info_list), e_info_list)
        # ���ص�len(e_info)�� len(flow_id_list)���ʱ��������ȱʧ�����к������ж�
        # if len(e_info_list) == len(flow_id_list):  # ����-1
        if len(e_info_list) == len(self.file_flowid_count()):  # ��������flowid��һ��
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
                    print("------��ʼ��%s ����״̬���ж�------\n" % e_id)
                    while e_final_status in ("READY", "RUNNING"):
                        print("------����whileѭ��------\n")
                        # ״̬Ϊ ready ���� RUNNINGʱ���ٴβ�ѯe_final_status
                        print("------��ʼ�ȴ�20S------\n")
                        time.sleep(10)
                        # ����get_e_finial_status(e_scheduler_id)�ٴβ�ѯ״̬
                        e_info = self.get_e_finial_status(e_scheduler_id)
                        # ��e_final_status ���¸�ֵ
                        e_final_status = e_info["e_final_status"]
                        print("------�ٴβ�ѯ���e_final_status: %s------\n" %e_final_status)
                        # time.sleep(50)
                    if e_final_status == "FAILED":
                        print("execution %s ִ��ʧ��" % e_id)
                        sink_dataset_dict["e_final_status"] = e_final_status
                        sink_dataset_dict["o_dataset"] = ""
                        sink_dataset_list.append(sink_dataset_dict)
                        # continue
                    elif e_final_status == "KILLED":
                        print("execution %s ��ɱ��" % e_id)
                        sink_dataset_dict["e_final_status"] = e_final_status
                        sink_dataset_dict["o_dataset"] = ""
                        sink_dataset_list.append(sink_dataset_dict)
                        # continue
                    elif e_final_status == "SUCCEEDED":
                        # �ɹ����ѯflow_execution_output���е�dataset, ��sink��Ӧ�����dataset��ȡ��dataset id �����ظ�ID����������Ԥ���ӿڲ鿴����`
                        # print("��ѯdata_json_sql:")
                        sink_dataset_dict["e_final_status"] = e_final_status
                        print(e_final_status, e_id)
                        data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_id
                        # print(data_json_sql)
                        data_json = self.ms.ExecuQuery(data_json_sql)
                        # print("��ӡdata_json:", data_json)
                        for n in range(len(data_json)):
                            sink_dataset = data_json[n]["dataset_json"]  # ���ؽ��ΪԪ��
                            print('-----sink_dataset-----', sink_dataset)
                            # print('sink_dataset:', sink_dataset)
                            if sink_dataset:
                                sink_dataset_id = dict_res(sink_dataset)["id"]  # ȡ��json���е�dataset id
                                sink_dataset_dict["o_dataset"] = sink_dataset_id
                                d = json.loads(json.dumps(sink_dataset_dict))
                                sink_dataset_list.append(d)
                            else:
                                continue

                        print('��%d�ε�sink_dataset_list %s' % (i, sink_dataset_list))
                    else:
                        print("���ص�execution ִ��״̬����ȷ")
                        return
                else:
                    print("execution������")
                    return
            print('��󷵻ص�sink_dataset_list\n', sink_dataset_list)
            print("------check_out_put(self)ִ�н���------\n")
            # ����get_json()������ȡdataset_json
            return sink_dataset_list
        else:
            print("���ص�scheduler_id_listֵȱʧ")
            return

    def get_json(self):
        print("------��ʼִ��get_json()------\n")
        sink_dataset = self.check_out_put()
        print('��ӡsink_dataset', sink_dataset)
        sink_dataset_json = []
        # ---ʹ��openpyxl������ 12.26update---
        flow_table = load_workbook(abs_dir("flow_dataset_info.xlsx"))
        # info_sheet_names = flow_table.get_sheet_names()
        flow_sheet = flow_table.get_sheet_by_name('flow_info')
        sheet_rows = flow_sheet.max_row  # ��ȡ����

        # ͨ��datasetԤ���ӿ�ȡ�����ݵ�Ԥ��json�� result.text
        for i in range(0, len(sink_dataset)):
            dataset_id = sink_dataset[i]["o_dataset"]
                # ���dataset_id��ȣ�# ��output_dataset ��Ԥ������json��д��ʵ�ʽ����
                # ������������ѭ��
            for j in range(2, sheet_rows+1):
                # ��һ��:����ϴεĲ��Խ��
                flow_sheet.cell(row=j, column=5, value='')
                flow_sheet.cell(row=j, column=6, value='')
                flow_sheet.cell(row=j, column=8, value='')
                flow_sheet.cell(row=j, column=9, value='')
                flow_sheet.cell(row=j, column=10, value='')
                # �ڶ������ж�dataset id�Ƿ���ڣ�������ȡ��Ԥ��������ҵ�������ȵ�dataset id��д��Ԥ�����
                if dataset_id:
                    print('dataset_id:', dataset_id)
                    # ͨ��datasetԤ���ӿڣ���ȡdataset json��
                    priview_url = "%s/api/datasets/%s/preview?rows=5000&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (
                    HOST_189, dataset_id)
                    result = requests.get(url=priview_url, headers=get_headers())
                    # print('Ԥ���ӿڷ��ص�dataset json��', '\n', result.json())
                    if sink_dataset[i]["flow_id"] == flow_sheet.cell(row=j, column=2).value:
                        # print(sink_dataset[i]["flow_id"])
                        flow_sheet.cell(row=j, column=5, value=sink_dataset[i]["execution_id"])
                        flow_sheet.cell(row=j, column=6, value=sink_dataset[i]["e_final_status"])

                    else:
                        for t in range(j, 2, -1):
                            if sink_dataset[i]["flow_id"] == flow_sheet.cell(row=t-1, column=2).value:
                                flow_sheet.cell(row=j, column=5, value=sink_dataset[i]["execution_id"])
                                flow_sheet.cell(row=j, column=6, value=sink_dataset[i]["e_final_status"])
                                # flow_sheet.cell(row=j, column=8, value=result.text)  # ʵ�ʽ��д����
                                break
                # ���dataset id��Ⱦ�д��ʵ�ʽ��������Ⱦ�������
                    for n in range(j, sheet_rows+1):
                        if dataset_id == flow_sheet.cell(row=j, column=4).value:
                            flow_sheet.cell(row=j, column=8, value=result.text)  # dataset id ��ȣ�ʵ�ʽ��д����
                else:
                    if sink_dataset[i]["flow_id"] == flow_sheet.cell(row=j, column=2).value:
                        # print(sink_dataset[i]["flow_id"])
                        flow_sheet.cell(row=j, column=5, value=sink_dataset[i]["execution_id"])
                        flow_sheet.cell(row=j, column=6, value=sink_dataset[i]["e_final_status"])

                    else:
                        for t in range(j, 2, -1):
                            if sink_dataset[i]["flow_id"] == flow_sheet.cell(row=t - 1, column=2).value:
                                flow_sheet.cell(row=j, column=5, value=sink_dataset[i]["execution_id"])
                                flow_sheet.cell(row=j, column=6, value=sink_dataset[i]["e_final_status"])
                                # flow_sheet.cell(row=j, column=8, value=result.text)  # ʵ�ʽ��д����
                                break

        flow_table.save(abs_dir("flow_dataset_info.xlsx"))
        # copy_table.save(abs_dir("flow_dataset_info.xlsx"))
        # ---ʹ��openpyxl������ 12.26update---
        table = load_workbook(abs_dir("flow_dataset_info.xlsx"))
        table_sheet = table.get_sheet_by_name('flow_info')
        c_rows = table_sheet.max_row

        # mode = overwrite:ʵ�ʽ��д���󣬶Ա�Ԥ�ڽ����ʵ�ʽ��,����ʧ��������� fail_detail
        print('-----��ʼ�ԱȽ��----')
        for i in range(2, c_rows+1):
            table_sheet.cell(row=i, column=1, value=i-1)
            # ��flow_id = '09296a11-5abf-4af4-a58f-2f14e414db67'��ִ�н�������ж�
            if table_sheet.cell(row=i, column=2).value == '09296a11-5abf-4af4-a58f-2f14e414db67':
                if table_sheet.cell(row=i, column=6).value == 'SUCCEEDED' and table_sheet.cell(row=i, column=8):
                    new_result = []
                    expect_result = list(eval(table_sheet.cell(row=i, column=7).value))
                    actual_result = list(eval(table_sheet.cell(row=i, column=8).value))
                    for b_item in range(len(expect_result)):
                        for a_item in range(len(actual_result)):
                            if actual_result[a_item] == expect_result[b_item]:
                                new_result.append(actual_result[a_item])
                    if new_result == actual_result:
                        table_sheet.cell(row=i, column=9, value="pass")
                        print('test_result:', table_sheet.cell(row=i, column=9).value)
                        table_sheet.cell(row=i, column=10, value="")
                    else:
                        table_sheet.cell(row=i, column=9, value="fail")
                        table_sheet.cell(row=i, column=10, value="execution: %s Ԥ�ڽ��ʵ�ʽ����һ�� " %
                                                                 (table_sheet.cell(row=i, column=5).value))
                elif table_sheet.cell(row=i, column=5).value == 'SUCCEEDED' and table_sheet.cell(row=i, column=8) == "":
                    table_sheet.cell(row=i, column=9, value="fail")
                    table_sheet.cell(row=i, column=10, value="execution: %s Ԥ�ڽ��ʵ�ʽ����һ��,ʵ�ʽ��Ϊ�� " %
                                                             (table_sheet.cell(row=i, column=5).value))
                elif table_sheet.cell(row=i, column=5).value == 'FAILED':
                    table_sheet.cell(row=i, column=9, value="fail")
                    table_sheet.cell(row=i, column=10, value="execution: %s ִ��״̬Ϊ %s" % (
                        table_sheet.cell(row=i, column=5).value, table_sheet.cell(row=i, column=6).value))
                else:
                    print('��ȷ��flow_id: %s��ִ��״̬' % table_sheet.cell(row=i, column=2).value)

            else:
                # �ж�modeΪoverwrite
                if table_sheet.cell(row=i, column=11).value == 'overwrite':  # �ж�mode
                    # ʵ�ʽ�����ڲ���ִ�н��Ϊsucceeded
                    if table_sheet.cell(row=i, column=8).value and table_sheet.cell(row=i, column=6).value == "SUCCEEDED":
                        # va7ΪԤ�ڽ����va8Ϊʵ�ʽ�����������������Ա��Ƿ����
                        va7 = list(eval(table_sheet.cell(row=i, column=7).value))
                        va8 = list(eval(table_sheet.cell(row=i, column=8).value))
                        if va7 != []:
                            va7_k = va7[0].keys()
                            va7_key = list(va7_k)
                            # print('va7_key', va7_key)
                            S_va7 = sorted(va7, key=lambda item: item[va7_key[0]], reverse=True)    # û�� idʱ�������
                            S_va8 = sorted(va8, key=lambda item: item[va7_key[0]], reverse=True)
                            # print('flow_id', table_sheet.cell(row=i, column=2).value)
                            # print(S_va7, '\n', S_va8)
                            print('-----ȷ�Ͻ��------')
                            if S_va7 == S_va8:
                                table_sheet.cell(row=i, column=9, value="pass")
                                print('test_result:', table_sheet.cell(row=i, column=9).value)
                                table_sheet.cell(row=i, column=10, value="")
                            else:
                                table_sheet.cell(row=i, column=9, value="fail")
                                table_sheet.cell(row=i, column=10, value="execution: %s Ԥ�ڽ��ʵ�ʽ����һ�� " %
                                                                         (table_sheet.cell(row=i, column=5).value))
                        elif va7 == [] and va8 == []:
                            table_sheet.cell(row=i, column=9, value="pass")
                        else:
                            print("Ԥ�ڽ��Ϊ�գ��޷���ȡ����key")

                    elif table_sheet.cell(row=i, column=6).value == "FAILED":
                        table_sheet.cell(row=i, column=9, value="fail")
                        table_sheet.cell(row=i, column=10, value="execution: %s ִ��״̬Ϊ %s" % (
                            table_sheet.cell(row=i, column=5).value, table_sheet.cell(row=i, column=6).value))
                        # else:
                        # print('execution: %sִ��״̬Ϊ�գ���˲�' % table_sheet.cell(i, 3).value)
                        # copy_table.save('flow_dataset_info.xls')
                    else:
                        table_sheet.cell(row=i, column=9, value="fail")
                        table_sheet.cell(row=i, column=10, value="����������datasetID��д����")
                # �ж�modeΪappend
                elif table_sheet.cell(row=i, column=11).value == 'append':
                    if table_sheet.cell(row=i, column=8).value and table_sheet.cell(row=i, column=6).value == "SUCCEEDED":  # ʵ�ʽ������
                        expect_result_list = list(eval(table_sheet.cell(row=i, column=7).value))
                        expect_len = len(expect_result_list)
                        actual_result_list = list(eval(table_sheet.cell(row=i, column=8).value))

                        if expect_result_list == actual_result_list[-expect_len:]:  # ʵ�ʽ����Ƭ��Ԥ�ڽ������һ�µ����ݣ��жϺ�Ԥ�ڽ���Ƿ����
                            # print('expect_result_list:', expect_result_list)
                            # print('actual_result_list:', actual_result_list)
                            # print(expect_result_list == actual_result_list[-expect_len:])
                            table_sheet.cell(row=i, column=9, value="pass")
                            table_sheet.cell(row=i, column=10, value="")
                        else:
                            table_sheet.cell(row=i, column=9, value="fail")
                            table_sheet.cell(row=i, column=10,
                                                   value="execution: %s Ԥ�ڽ��ʵ�ʽ����һ�� \nԤ�ڽ��: %s\nʵ�ʽ��: %s" % (
                                                   table_sheet.cell(row=i, column=5).value,
                                                   table_sheet.cell(row=i, column=7).value,
                                                   table_sheet.cell(row=i, column=8).value))
                    elif table_sheet.cell(row=i, column=6).value == "FAILED":  # executionִ��ʧ��
                        table_sheet.cell(row=i, column=9, value="fail")
                        table_sheet.cell(row=i, column=10,
                                               value="execution: %s ִ��״̬Ϊ %s" % (
                                               table_sheet.cell(row=i, column=5).value, table_sheet.cell(row=i, column=6).value))
                    else:
                        table_sheet.cell(row=i, column=9, value="fail")
                        table_sheet.cell(row=i, column=10, value="����������datasetID��д����")

                else:
                    table_sheet.cell(row=i, column=9, value="fail")
                    table_sheet.cell(row=i, column=10, value="��ȷ��flow��mode")
        # print(table_sheet.cell(row=2, column=8).value)
        # print(table_sheet.cell(row=2, column=9).value)
        table.save(abs_dir("flow_dataset_info.xlsx"))

        print('����������')


if __name__ == '__main__':
    GetCheckoutDataSet()



    # threading.Timer(1500, get_headers()).start()
    # g.data_for_create_scheduler()



