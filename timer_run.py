import schedule
import time
def job():   # 定时任务
    print("I'm working...")
    now_time=time.time()
    time_interval=6*60*60 # 6小时的历史数据
    history_time=now_time-time_interval
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(history_time)))# 当前时间


schedule.every().hour.at('16:05').do(job)  # 在每小时的00分00秒开始，定时任务job
while True:
    schedule.run_pending()
    time.sleep(1)