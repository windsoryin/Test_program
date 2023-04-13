
import datetime
import time

import pandas as pd
import calendar
# 闰秒
LEAP_SECONDS = 18

# 输入：GPS周、GPS周内秒、闰秒（可选，gps时间不同，闰秒值也不同，由Leap_Second.dat文件决定）
# 输出：UTC时间（格林尼治时间）
# 输入示例： gps_week_seconds_to_utc(2119, 214365.000)
# 输出示例： '2020-08-18 11:32:27.000000'
# def gps_week_seconds_to_utc(gpsweek, gpsseconds, leapseconds=LEAP_SECONDS):
#     datetimeformat = "%Y-%m-%d %H:%M:%S.%f"
#     epoch = datetime.strptime("1980-01-06 00:00:00.000", datetimeformat)
#     # timedelta函数会处理seconds为负数的情况
#     elapsed = datetime.timedelta(days=(gpsweek*7), seconds=(gpsseconds-leapseconds))
#     return datetime.strftime(epoch+elapsed, datetimeformat)

if __name__ == "__main__":
    # print(gps_week_seconds_to_utc(2257,288159))
    # assigned regular string date
    date_time = datetime.datetime(2021, 7, 26, 21, 20)
    # print regular python date&time
    print("date_time =>", date_time)
    # displaying unix timestamp after conversion
    unix=calendar.timegm(date_time.timetuple())
    print("unix_timestamp => ",
          unix)
    print(pd.to_datetime(unix,unit='s'))