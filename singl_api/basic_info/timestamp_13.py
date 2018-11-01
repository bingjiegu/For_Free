import time
import pymysql
from basic_info import data_from_db


def timestamp_to_13(time_stamp, digits=13):
    time_stamp = data_from_db.schema()
    time_stamp = time_stamp.strftime('%a %b %d %H:%M:%S %Y')
    time_stamp = time.mktime(time.strptime(time_stamp))
    digits = 10 ** (digits - 10)
    time_stamp = int(round(time_stamp*digits))
    return time_stamp






print(timestamp_to_13(13))
