import base64


def encrypt_rf(enstr):
    """加密方法：base64"""
    new_enstr = bytes(enstr, encoding="utf8")
    test_enstr = base64.b64encode(new_enstr)
    return test_enstr



# enstr = '123456'
# print(encrypt_rf(enstr=enstr))