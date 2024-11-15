import pandas as pd
from datetime import datetime, timedelta
import os

def create_script_status_file(script_name,freqency,prcess_name,server_name,starttime,end_time):
    run_date = datetime.now().strftime('%Y%m%d')
    daily_tgt_file_path = f"D:\\script_monitoring\\daily_scripts"
    
    os.chdir (daily_tgt_file_path)
    if not os.path.exists(run_date):
        os.mkdir(run_date)

    data = {'scriptname' :f'{script_name}.py',
    'freq' : freqency,
    'process' : prcess_name,
    'server' : server_name,
    'starttime' : starttime,
    'endtime' : end_time}

    df = pd.DataFrame([data])
    df['starttime'] = pd.to_datetime(df['starttime'])
    df['endtime'] = pd.to_datetime(df['endtime'])
    time_diff= df['endtime'] - df['starttime']
    df['total_completion_time'] = time_diff.apply(lambda x: f"{x.components.hours} hours {x.components.minutes} minutes")

    df.to_csv(f"{daily_tgt_file_path}\\{run_date}\\{script_name}_{run_date}.csv",index=False)