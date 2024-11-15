import psutil
import time
import sys
from datetime import datetime
import subprocess
import os

counter=0
timeout=60
normal_disk_space=79
alert_disk_space=90
run_dt = datetime.now().strftime('%Y%m%d%H%M')
run_date = datetime.now().strftime('%Y%m%d')
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
script_path = "D:\\oracle\\diskspace"
log_dir = "D:\\oracle\\diskspace\\log"
log_file_email = f"{log_dir}\\email_notication_{run_date}.log"
os.chdir(script_path)


#Wrapper script log
sys.stdout = open(f"{log_dir}\\diskspace_{run_date}.log" ,'a')
print("\n")
print("*********************************************************")
print("start time is : ",start_time)

# fuction which checks the diskspace of all the mounting disk.
def disk_space():
    usage_info={}
    for partition in psutil.disk_partitions():
        if 'cdrom' in partition.opts or 'tmpfs' in partition.device:
            continue
        usage = psutil.disk_usage(partition.mountpoint)
        usage_info[partition.device] = usage.percent
        #print(f"{usage.percent}% {partition.device}")
    return usage_info


#function which check after getting alert mail disk is normalized or not.
def normalize(counter,timeout,normal_disk_space):
    print("first",counter)
    while counter < timeout:
        usage_info = disk_space()
        max_key=max( usage_info,key=usage_info.get)  
        max_value=usage_info[max_key]
    
        print("max_key is :",max_key)
        print("max value is :",max_value)
    
    
        formatted_dict = '\n'.join(f'{k} - {v} %' for k, v in usage_info.items())
        #print(formatted_dict)
        
        if max_value <= normal_disk_space:
            print("Disk space normallized after file removed")
            email_start_subject = f"Disk space utilization on windows VM has normalized  for {run_dt}"
        #print(email_start_subject)
            email_start_body = f"""
Disk space utilization on windows(CBIIndia.global.local) VM is normalized to 
{formatted_dict}. 
"""
            config_file = "email_config.config"

            with open(log_file_email, "a") as f:
                subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
                sys.exit()#send mail exit script
        else:
            counter=counter+1
            #print(counter)
            time.sleep(60)
    return counter
            

#calling disk_space function and store the output of disk space in variable.
usage_info = disk_space() 

#check the maximum disk space value and compair with alert_msg.
max_key = max( usage_info,key=usage_info.get)  
max_value = usage_info[max_key]

print("max_key is :",max_key)
print("max value is :",max_value)


formatted_dict = '\n'.join(f'{k} - {v} %' for k, v in usage_info.items())
print(formatted_dict)


if max_value > alert_disk_space:
    print("Running out of space")
    #send mail and call function
    email_start_subject = f"Alert: running out of disk space on windows for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
Running out of space on windows(CBIIndia.global.local) server.
{formatted_dict}  
"""
    config_file = "email_config.config"

    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
        
    counter=normalize(counter,timeout,normal_disk_space)
else:
    print("diskspace is normal")
    sys.exit()
    #exit the script



while True:
    counter1 = counter 
    if counter1 == timeout:
        print("still diskspace above threshold")
        email_start_subject = f"Alert: disk space on windows is still above threshold (90%) for {run_dt}"
        #print(email_start_subject)
        email_start_body = f"""
Disk space utilization on windows(CBIIndia.global.local) VM is still at 
{formatted_dict}
"""
        config_file = "email_config.config"

        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
        counter = 0
        normalize(counter,timeout,normal_disk_space)
    else:
        print("still diskspace above threshold")
        email_start_subject = f"Alert: disk space on windows is still above threshold (90%) for {run_dt}"
        #print(email_start_subject)
        email_start_body = f"""
Disk space utilization on windows(CBIIndia.global.local) VM is still at 
{formatted_dict}
"""
        config_file = "email_config.config"

        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
        normalize(counter,timeout,normal_disk_space)
    