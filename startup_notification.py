import os
import time
from datetime import datetime,timedelta
import subprocess
import sys, getopt
import re
import json


run_date = datetime.now().strftime('%Y%m%d')
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
script_path = f"D:\\dstdw\\windows_restart"
log_dir=f"{script_path}\\log"
log_file_email = f"{log_dir}\\email_notification_{run_date}.log"

sys.stdout = open(f"{log_dir}\\startup_notification_{run_date}.log", 'a')
sys.stderr = open(f"{log_dir}\\startup_notification_{run_date}.log", 'a')

os.chdir(script_path)

print(f"***********************************************")
print(f"run date is : {run_date}")
print(f"script path is : {script_path}")
print(f"script start time : ", start_time)
print('\n')

email_start_subject = f"After restart windows vm is now active for {run_date}"
#print(email_start_subject)
email_start_body = f"""After restart windows vm is now active for {run_date}."""
config_file = "email_config.config"
with open(log_file_email, "a") as f:
    subprocess.run(["python",f"{script_path}/email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
message=f"After restart windows vm is now active for {run_date}"
payload = {"text": message}
payload_str = json.dumps(payload)
curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str, "https://o365spi.webhook.office.com/webhookb2/f0b33c31-1285-41b8-8866-c0b7c3033a86@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/ce4d8b46304b4c97a1c6bf81f8b70b79/d9187cfe-54d7-4f26-bc30-86cde4772755/V2YbfWi6qDI7NtyXSMvGbIqT38sdZjghf_e4lOSVUVmyQ1"]
subprocess.run(curl_command1)