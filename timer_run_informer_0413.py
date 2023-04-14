from pandasticsearch import DataFrame
import pandas as pd
import numpy as np
import time
import csv
import datetime
import os
import subprocess
import json
from elasticsearch6 import Elasticsearch
import calendar
import schedule
import time
from math import *

# pandas打印所有列，生产环境可以去掉
pd.set_option('display.max_columns', None)
"""
url：
外网地址:http://42.193.247.49:9200   内网地址: http://172.16.20.3:9200

index：
目前已经调试通过2个
1、HWS气象数据：hws@*
2、GNSS数据：gnss@*

username：
账号：gnss

password：
密码：YKY7csX#

verify_ssl：校验SSL证书 False

compat：兼容elasticsearch版本为6.8.2
"""

global seq_len
global site_name


# 定时系统
def job():  # 定时任务
    print("I'm working...")
    now_time = time.time()  # 程序错误，导致需要手动减8h
    time_interval = seq_len / 12 * 60 * 60  # 8小时的历史数据 = seqlen=96
    history_time = now_time - time_interval
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(history_time)))  # 当前时间
    get_data(now_time, history_time)
    run_model()
    write_database(site_name)
    print('successfully done')


def read_database(index_name, start_t, end_t):
    # 读取数据库数据
    # index_name：
    # 1、HWS气象数据：hws @ *
    # 2、GNSS数据：gnss @ *
    df = DataFrame.from_es(url='http://42.193.247.49:9200', index=index_name,
                           username="gnss", password="YKY7csX#", verify_ssl=False,
                           compat=6)
    # 固定为doc
    df._doc_type = "doc"
    # 打印schema 方便查看当前数据格式，生产环境去掉，
    # 目前数据格式中除了原本设备数据 还包含 两个时间字段 time设备数据字段  timestamp时序库时间字段（实际查询建议用此字段）。
    df.print_schema()
    # 查询事例  1、filter：时间过滤、设备过滤； 2、select查询所有字段  3、sort以什么字段进行排序  4、限制返回数量
    # 其他用法参考：https://github.com/onesuper/pandasticsearch
    data = df.filter((df['timestamp'] > start_t) & (df['timestamp'] < end_t) & (df['device'] == 'B04')) \
        .select(*df.columns) \
        .sort(df["timestamp"].asc) \
        .limit(100000) \
        .to_pandas()
    return data


def dynamic_window(winsize, win_max, gnss_data, hws_data, resample_time):
    flag = 1
    while flag:
        t1 = resample_time - winsize  # i*5*60 5min interval；  【-0.5*60，0.5*60】window length
        t2 = resample_time + winsize
        tmp_data = gnss_data[(gnss_data['timestamp'] > int(t1)) & (gnss_data['timestamp'] < int(t2))]
        mean_data = tmp_data.loc[:, ['ztd', 'latitude', 'longitude', 'height']].mean(0)
        tmp_hwsdata = hws_data[(hws_data['timestamp'] > int(t1)) & (hws_data['timestamp'] < int(t2))]
        mean_hwsdata = (tmp_hwsdata.loc[:, ['Ta', 'Pa', 'Ta', 'Rc']].mean(0))
        winsize = winsize * 2
        if (tmp_data.shape[0]) >= 1 & tmp_hwsdata.shape[0] >= 1:
            flag = 0
        if winsize > win_max:
            flag = 0
    return mean_data, mean_hwsdata


def resampling(now_time, gnss_data, hws_data, winsize, win_max):
    # resample real_data and interpolate those missing values
    now_time_utc = pd.to_datetime(now_time, unit='s')
    near_minute = np.floor(now_time_utc.minute / 5) * 5
    end_time_utc = now_time_utc.replace(minute=near_minute.astype(int), second=0, microsecond=0)
    start_time_utc = end_time_utc - datetime.timedelta(minutes=seq_len * 5)  # 根据seqlen=96，得到起始时间
    # resample_time = pd.date_range(start=start_time_utc, end=end_time_utc, freq='5min') # 产生重采样时间点集合
    end_time_unix = calendar.timegm(end_time_utc.timetuple())
    resample_time = calendar.timegm(start_time_utc.timetuple())
    resamp_data = pd.DataFrame(None,
                               index=['t2m(k)', 'sp(Pa)', 'd2m(k)', 'tp', 'ztd', 'latitude', 'longitude', 'height'])

    for i in range(seq_len):
        re_time = resample_time + (i) * 5 * 60
        # 动态改变窗口大小，获取时间窗口内平均值
        mean_data, mean_hwsdata = dynamic_window(winsize, win_max, gnss_data, hws_data, re_time)
        # 拼接数据
        s = pd.concat([mean_hwsdata, mean_data], axis=0).to_frame()
        s.index = ['t2m(k)', 'sp(Pa)', 'd2m(k)', 'tp', 'ztd', 'latitude', 'longitude', 'height']
        resamp_data = pd.concat([resamp_data, s], axis=1, ignore_index=True)

    time_dt = pd.DataFrame(np.arange(resample_time, end_time_unix, 5 * 60))
    resamp_data.loc[resamp_data.shape[0]] = np.arange(resample_time, end_time_unix, 5 * 60)
    resamp_data = resamp_data.rename(index={resamp_data.shape[0] - 1: 'date'})
    # resamp_data.drop(columns=0)
    resamp_data = resamp_data.T
    resamp_data['date'] = pd.to_datetime(resamp_data['date'], unit='s')
    return resamp_data


def calc_pwv(ztd, t, p, lat, height):
    # t: temperature (k)
    # p: pressure (hpa)
    # lat: latitude (degree)
    # height: (m)
    # ztd: zenith tropospheric delay (m)
    # Saastamoinen model
    tm = 70.2 + 0.72 * t
    lat = lat / 180 * pi
    zhd0 = pow(10, -3) * (2.2768 * p / (1 - 0.00266 * np.cos(2 * lat) - 0.00028 * height * pow(10, -3)))  # 单位m
    zwd0 = pow(10, 3) * (ztd - zhd0)  # %单位mm
    k = pow(10, 6) / (4.613 * pow(10, 6) * (3.776 * pow(10, 5) / tm + 22.1))  # 单位Mkg/m^3
    k = k * pow(10, 6) / pow(10, 3)  # 单位换算kg/m^2=mm
    pwv = k * zwd0
    return pwv


def get_data(now_time, history_time):
    gnss = 'gnss@*'
    hws = 'hws@*'
    end_t = now_time
    start_t = history_time
    gnss_data = read_database(gnss, start_t, end_t)
    if len(gnss_data) > 0:
        gnss_data['timestamp'] = gnss_data['timestamp'].astype(int)
        gnss_data['ztd'] = gnss_data['ztd'].astype(float)
        gnss_data['latitude'] = gnss_data['latitude'].astype(float)
        gnss_data['longitude'] = gnss_data['longitude'].astype(float)
        gnss_data['height'] = gnss_data['height'].astype(float)
        gnss_data['time'] = pd.to_datetime(gnss_data['time'], unit='s')
    hws_data = read_database(hws, start_t, end_t)
    if len(hws_data) > 0:
        hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
        hws_data['timestamp'] = hws_data['timestamp'].astype(int)
        hws_data['Ta'] = hws_data['Ta'].astype(float)
        hws_data['Pa'] = hws_data['Pa'].astype(float)
        hws_data['Rc'] = hws_data['Rc'].astype(float)
        hws_data['Ua'] = hws_data['Ua'].astype(float)
    # hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
    # print(real_data)
    ##############
    resamp_data = resampling(now_time, gnss_data, hws_data, 30, 30 * 6)  # 重采样数据
    """
    读取数据，写入csv文件中
    """
    resamp_data.iloc[:, 0:8] = resamp_data.iloc[:, 0:8].interpolate(method='linear', order=1, limit=10, limit_direction='both')  #
    resamp_data["t2m(k)"] = resamp_data["t2m(k)"].astype(float)
    resamp_data["t2m(k)"] = resamp_data[["t2m(k)"]].apply(lambda x: x["t2m(k)"] + 273.15, axis=1)
    resamp_data["d2m(k)"] = resamp_data["d2m(k)"].astype(float)
    resamp_data["d2m(k)"] = resamp_data[["d2m(k)"]].apply(lambda x: x["d2m(k)"] + 274, axis=1)
    resamp_data["sp(Pa)"] = resamp_data["sp(Pa)"].astype(float)
    # calculate PWV
    ztd_data = resamp_data.loc[:, 'ztd']
    t_data = resamp_data.loc[:, 't2m(k)']
    p_data = resamp_data.loc[:, 'sp(Pa)']
    lat_data = resamp_data.loc[:, 'latitude']
    h_data = resamp_data.loc[:, 'height']
    pwv = calc_pwv(ztd_data, t_data, p_data, lat_data, h_data)
    #
    data_csv = resamp_data[['date', 't2m(k)', 'sp(Pa)', 'd2m(k)']]  # 重组数据
    data_csv.insert(4, 'pwv', pwv)
    data_csv.insert(5, 'tp', resamp_data.loc[:,'tp'])     #重组数据

    data_csv.to_csv('./real_data/{}.csv'.format(site_name), index=False)
    print('done')


def run_model():
    # 通过subprocess.popen 执行 命令行命令
    p = subprocess.Popen(['python test02.py'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print(line)
    retval = p.wait()


def write_database(site_name):
    loaddata = np.load(
        './results/informer_ftMS_sl96_ll48_pl24_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_{}.npy'.format(
            site_name))

    es = Elasticsearch(hosts=["42.193.247.49:9200"], http_auth=('gnss', 'YKY7csX#'),
                       scheme="http")
    action_body = ''
    for i in range(len(loaddata)):
        tmp_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tmp_rf = loaddata[0, i, 0].astype(float)
        param_index = {"index": {"_type": "doc"}}
        param_data = {"time": tmp_time, "predict_rainfall": tmp_rf}
        action_body += json.dumps(param_index) + '\n'
        action_body += json.dumps(param_data) + '\n'
    print(action_body)


# schedule.every().hour.at('55:05').do(job)  # 在每小时的00分00秒开始，定时任务job
schedule.every(10).seconds.do(job)
seq_len = 12
site_name = 'wh_kc'
while True:
    schedule.run_pending()
    time.sleep(1)
