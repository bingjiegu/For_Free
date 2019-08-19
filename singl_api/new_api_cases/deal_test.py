# coding:utf-8
from new_api_cases.GET_requests import *
from new_api_cases.POST_requests import *
from new_api_cases.PUT_requests import *
from new_api_cases.DELETE_requests import *
import os
from util.get_host import url_host

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
print(ab_dir)

def test_001():
    print('tset001')
