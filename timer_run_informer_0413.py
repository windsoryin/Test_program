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
def job():   # 定时任务
    print("I'm working...")
    now_time=time.time() # 程序错误，导致需要手动减8h
    time_interval=seq_len/12*60*60 # 8小时的历史数据 = seqlen=96
    history_time=now_time-time_interval
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(history_time)))# 当前时间
    get_data(now_time, history_time)
    run_model()
    write_database(site_name)
    print('successfully done')

def read_database(index_name,start_t,end_t):
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
    data = df.filter((df['timestamp'] > start_t) & (df['timestamp'] < end_t) & (df['device'] == 'B04'))\
        .select(*df.columns)\
        .sort(df["timestamp"].asc)\
        .limit(100000)\
        .to_pandas()
    return data

def dynamic_window(winsize,win_max,gnss_data,hws_data,resample_time):
    flag = 1
    while flag:
        t1 = resample_time - winsize  # i*5*60 5min interval；  【-0.5*60，0.5*60】window length
        t2 = resample_time + winsize
        tmp_data = gnss_data[(gnss_data['timestamp'] > int(t1)) & (gnss_data['timestamp'] < int(t2))]
        mean_data = tmp_data.loc[:, ['ztd', 'latitude', 'longitude']].mean(0)
        tmp_hwsdata = hws_data[(hws_data['timestamp'] > int(t1)) & (hws_data['timestamp'] < int(t2))]
        mean_hwsdata = (tmp_hwsdata.loc[:, ['Ta', 'Pa', 'Ta', 'Rc']].mean(0))
        winsize = winsize * 2
        if (tmp_data.shape[0])>=1 & tmp_hwsdata.shape[0]>=1:
            flag = 0
        if winsize > win_max:
            flag = 0
    return mean_data,mean_hwsdata

def resample_data(now_time, gnss_data, hws_data, winsize,win_max):
    # resample real_data and interpolate those missing values
    now_time_utc = pd.to_datetime(now_time, unit='s')
    near_minute = np.floor(now_time_utc.minute / 5) * 5
    end_time_utc = now_time_utc.replace(minute=near_minute.astype(int),second=0,microsecond=0)
    start_time_utc = end_time_utc-datetime.timedelta(minutes=seq_len*5) # 根据seqlen=96，得到起始时间
    #resample_time = pd.date_range(start=start_time_utc, end=end_time_utc, freq='5min') # 产生重采样时间点集合
    end_time_unix = calendar.timegm(end_time_utc.timetuple())
    resample_time = calendar.timegm(start_time_utc.timetuple())
    resamp_data =pd.DataFrame(None, index=['t2m(k)','sp(Pa)','d2m(k)','tp','ztd','latitude', 'longitude'])
    for i in range(seq_len):
        re_time=resample_time + (i)*5*60
        mean_data, mean_hwsdata = dynamic_window(winsize, win_max, gnss_data, hws_data, re_time)
        s = pd.concat([mean_hwsdata, mean_data], axis=0).to_frame()
        s.index=['t2m(k)','sp(Pa)','d2m(k)','tp','ztd','latitude', 'longitude']
        resamp_data = pd.concat([resamp_data, s], axis=1, ignore_index=True)
    time_dt = pd.DataFrame(np.arange(resample_time,end_time_unix,5*60))
    resamp_data.loc[resamp_data.shape[0]]=np.arange(resample_time,end_time_unix,5*60)
    resamp_data = resamp_data.rename(index={7:'date'})
    # resamp_data.drop(columns=0)
    resamp_data = resamp_data.T
    resamp_data['date'] = pd.to_datetime(resamp_data['date'], unit='s')
    return resamp_data

def get_data(now_time,history_time):
    gnss = 'gnss@*'
    hws = 'hws@*'
    end_t = now_time
    start_t = history_time
    gnss_data = read_database(gnss, end_t, end_t)
    if len(gnss_data)>0:
        gnss_data['timestamp']=gnss_data['timestamp'].astype(int)
        gnss_data['ztd'] = gnss_data['ztd'].astype(float)
        gnss_data['latitude'] = gnss_data['latitude'].astype(float)
        gnss_data['longitude'] = gnss_data['longitude'].astype(float)
        gnss_data['time'] = pd.to_datetime(gnss_data['time'], unit='s')
        ztd_data=gnss_data.loc[:,'ztd']
    hws_data = read_database(hws, start_t, end_t)
    if len(hws_data) > 0:
        hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
        hws_data['timestamp'] = hws_data['timestamp'].astype(int)
        hws_data['Ta'] = hws_data['Ta'].astype(float)
        hws_data['Pa'] = hws_data['Pa'].astype(float)
        hws_data['Rc'] = hws_data['Rc'].astype(float)
        hws_data['Ua'] = hws_data['Ua'].astype(float)
    #hws_data['time'] = pd.to_datetime(hws_data['time'], unit='s')
    # print(real_data)
    ##############
    resamp_data = resample_data(now_time, gnss_data, hws_data,30,30*6)    # 重采样数据
    """
    读取数据，写入csv文件中
    """
    data_csv=resamp_data[['date','t2m(k)','sp(Pa)','d2m(k)','ztd','tp']]  #重组数据
    # data_csv.insert(4, 'ztd', ztd_data)
    # data_csv.insert(5, 'tp', hws_data.loc[:,'Rc'])     #重组数据
    data_csv.columns=['date','t2m(k)','sp(Pa)','d2m(k)','pwv','tp']  #修改列名
    data_csv["t2m(k)"] = data_csv["t2m(k)"].astype(float)
    data_csv["t2m(k)"] = data_csv[["t2m(k)"]].apply(lambda x: x["t2m(k)"] + 273, axis=1)
    data_csv["d2m(k)"] = data_csv["d2m(k)"].astype(float)
    data_csv["d2m(k)"] = data_csv[["d2m(k)"]].apply(lambda x: x["d2m(k)"] + 274, axis=1)
    data_csv["sp(Pa)"] = data_csv["sp(Pa)"].astype(float)
    data_csv["sp(Pa)"] = data_csv[["sp(Pa)"]].apply(lambda x: x["sp(Pa)"]*100, axis=1) # unit from hpa to pa

    data_csv.iloc[:,1:6] = data_csv.iloc[:,1:6].interpolate(method='linear')
    data_csv.to_csv('./real_data/{}.csv'.format(site_name), index=False)
    print('done')
def run_model():
    p = subprocess.Popen(['python test02.py'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print(line)
    retval = p.wait()

def write_database(site_name):
    loaddata = np.load(
        './results/informer_ftMS_sl96_ll48_pl24_dm512_nh8_el2_dl1_df2048_atprob_fc5_ebtimeF_dtTrue_mxTrue_test_0/real_prediction_{}.npy'.format(site_name))

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

#schedule.every().hour.at('55:05').do(job)  # 在每小时的00分00秒开始，定时任务job
schedule.every(10).seconds.do(job)
seq_len = 96
site_name='wh_kc'
while True:
    schedule.run_pending()
    time.sleep(1)



