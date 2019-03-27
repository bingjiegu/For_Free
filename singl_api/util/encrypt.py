import base64
import json


class MyEncoder(json.JSONEncoder):
    def my_encoder(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def encrypt_rf(enstr):
    """加密方法：base64"""
    bytes_enstr = bytes(enstr, encoding="utf8")
    test_enstr = base64.b64encode(bytes_enstr)
    str_enstr = MyEncoder().my_encoder(test_enstr)

    return str_enstr


#
# enstr = 'default'
# print(encrypt_rf(enstr=enstr))
