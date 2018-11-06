import json
from json.decoder import JSONDecodeError


# 将res从str转化为dict
def dict_res(res):
    if res:
        if isinstance(res, str):
            try:
                res = json.loads(res)
                return res
            except JSONDecodeError as e:
                # raise e
                print("返回值格式转化错误: %s" % e)
        elif isinstance(res, dict):
            return res
        else:
            print("返回值类型无法转化为dictionary")
    else:
        print("没有返回值或返回值为空")





