import time
import random
import csv


now_time_len = time.time()
time_interval = 30  # 30s的历史数据
print(now_time_len)

future_window_len = now_time_len + time_interval
print(future_window_len)
# 统计时间最大为5分钟
interval = 1 * 60

max_future_window_len = now_time_len + interval
print(max_future_window_len)
# 窗口内数据初始为空
window_data = []

while True:
    # 判断是否需要重新计算平均值
    if len(window_data) == 0:
        # 如果窗口长度已经达到5分钟，如果没有数据，则置nan并重置窗口长度
        if time.time() >= max_future_window_len:
            print('nan')
        # 重置窗口数据为空
        window_data = []

    # 获取数据点
    data = random.randint(1, 10)

    # 将数据点加入窗口
    window_data.append(data)

    time1 = time.time()
    print(time1)
    # 如果窗口长度已到达指定长度，则计算平均值并输出结果
    if time1 >= future_window_len:
        average = sum(window_data) / len(window_data)
        print(average)

        with open(fr'time.csv', 'a', newline='') as fp:
                # 字段名
                writer = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow([average,
                                 ])

        # 重置窗口数据为空
        window_data = []

    # 暂停1秒钟，模拟实时获取数据
    time.sleep(1)



# print(window_data)