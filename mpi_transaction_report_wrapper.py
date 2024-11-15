import sys, getopt
from datetime import datetime, timedelta
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.api import Reports
import pandas as pd
import json
import pandas as pd
import os
import time
import glob
from sp_api.base import Marketplaces
import shutil
import subprocess
import re
import hashlib
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *


run_date = datetime.now().strftime('%Y%m%d')
prev_day = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d') #we are extracting data of this date
prev_date_f = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  
run_date_f = datetime.now().strftime('%Y-%m-%d')
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
tgt_s3="s3://desototech/Marketplace/Amazon/Transactions"
trigger_s3="s3://desototech/Marketplace/Amazon/Transactions/trigger_files"

script_path = "D:\\MPI\\Transactions"
log_dir=f"{script_path}\\log_files"
log_file_email = f"{log_dir}\\email_notification_{run_date}.log"
redshift_config = "amz_transaction_redshift.config"
trigger_dir=f"{script_path}\\trigger_files"

#src_s3="s3://cbi-marketplace/amazon_transactions"  #original S3 path
#tgt_s3="s3://desototech/DWH_team/Ashwini/Marketplace/Amazon/Transactions/20241022"
#trigger_s3="s3://desototech/DWH_team/Ashwini/Marketplace/Amazon/Transactions/trigger_files"
#run_date = '20241022'#script running on this date
#prev_day = '20241020' # script will extract this day's data (RUN_DATE-2)
#prev_date_f = '2024-10-21' # yesterday date in YYYY-MM-DD format (created since)
#run_date_f = '2024-10-22'  # script run_date in YYYY-MM-DD format  (created until)
#redshift_config = "amz_transaction_redshift_dev.config"


listOfGlobals = globals()
listOfGlobals['start_time'] = start_time
listOfGlobals['run_date'] = run_date
listOfGlobals['prev_day'] = prev_day
listOfGlobals['run_date_f'] = run_date_f
listOfGlobals['prev_date_f'] = prev_date_f
listOfGlobals['redshift_config'] = redshift_config
listOfGlobals['script_path'] = script_path
listOfGlobals['log_dir'] = log_dir
listOfGlobals['tgt_s3'] = tgt_s3
listOfGlobals['trigger_s3'] = trigger_s3
os.environ["ENV_DISABLE_DONATION_MSG"] = "1"

def send_start_end_mail(subject,body):
    log_file_email = f"{log_dir}\\email_notification_{run_date}.log"
    config_file = "email_config.config"
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",subject ,"--b",body, "--c", config_file], stdout=f, stderr=f)
        
def read_json_file():
    # Write the JSON object to a file
    json_path = f'{script_path}//credentials.json'
    with open(json_path) as json_file:
        json_data = json.load(json_file)
        
    refresh_token = json_data['refresh_token']
    lwa_app_id = json_data['lwa_app_id']
    lwa_client_secret = json_data['lwa_client_secret']
    
    return refresh_token,lwa_app_id,lwa_client_secret


def extract_and_download_file_through_api(mark_id, file_type,header):
    credentials_details=read_json_file()
    
    credentials = dict(
    refresh_token = credentials_details[0],
    lwa_app_id = credentials_details[1],
    lwa_client_secret = credentials_details[2],
    )
    
    #Pass the credentials and create report client to connect SPI-API
    reports=Reports(credentials=credentials)
    
    report_types = ["GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA"]
    
    #Get the report object and store it in variable
    next_token = None
    all_reports = []
    
    while True:
        try:
            # Prepare the parameters
            params = {}
            if next_token:
                params['nextToken'] = next_token
            else:
                params['reportTypes'] = report_types  
                params['createdSince'] = f"{prev_date_f}T00:00:01+00:00"  
                params['createdUntil'] = f"{run_date_f}T00:00:00+00:00"
                params['pageSize'] = 100
                params['marketplaceIds'] = mark_id    
                
            print(f"\nparameter is",params) 
            
            # Make the API call
            res = reports.get_reports(**params)
            
            reports_list = getattr(res, 'reports', [])
            all_reports.extend(reports_list)
            
            next_token = res.next_token
            
            print(f"\nnext token is :",next_token)
            time.sleep(10)
            # Break the loop if there are no more pages
            if not next_token:
                print("not getting token")
                break
                
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    
    df = pd.DataFrame(all_reports)
    
    if df.empty:
        # If the directory exists, delete it
        if os.path.exists(f"{script_path}\\modified_files\\{prev_day}\\{file_type}"):
            shutil.rmtree(f"{script_path}\\modified_files\\{prev_day}\\{file_type}")
        
        os.makedirs(f"{script_path}\\modified_files\\{prev_day}\\{file_type}")
        with open(f"{script_path}\\modified_files\\{prev_day}\\{file_type}\\mod_Amazon_{file_type}_Transaction_{prev_day}.csv",'w'):
            pass
        
        print(f"\nCopy modified Amazon transaction {file_type} file from Desoto VM to S3 started  at :", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        subprocess.run(["aws", "s3", "cp", f"{script_path}/modified_files/{prev_day}/{file_type}/", f"{tgt_s3}/Modified_files/{file_type}/{prev_day}","--exclude", "*","--include", "mod*", "--recursive"], shell=True)
        time.sleep(2)
    
        print(f"\nCopy blank modified Amazon transaction {file_type} file from Desoto VM to S3 completed  at :", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
        with open(f"{trigger_dir}\\amz_transactions_{file_type}_{prev_day}.txt",'w') as file:
            pass
    
        time.sleep(2)
        
        subprocess.run(["aws", "s3", "cp", f"{trigger_dir}/amz_transactions_{file_type}_{prev_day}.txt", f"{trigger_s3}/"], shell=True)
        
        print(f"\nTrigger file has been generated for Amazon Transaction {file_type}" )
        print(f"\nNo new file found for downloading for {file_type} so sending mail and exiting from here.....")
        email_alert_subject = f"Amazon MPI ({file_type}) Transactions files not extracted for {prev_day}"
        email_alert_body = f"""Amazon MPI ({file_type}) Transactions files were not extracted through API for the date {prev_day}.\nScript created a blank file for processing.\nThis script is scheduled on cbiankitw10.global.local VM."""
        send_start_end_mail(email_alert_subject,email_alert_body)
        return None  
    else:
        print(f"\nFound some files to download, so processing further...")
    
    df['processingEndTime'] = pd.to_datetime(df['processingEndTime'])
    
    # Sorted records according to processingEndTime
    df_sorted_desc = df.sort_values(by='processingEndTime', ascending=False)
    # reindex the df
    newidx = ["reportType","processingStartTime","processingEndTime","dataStartTime","dataEndTime","createdTime","processingStatus","marketplaceIds","reportDocumentId","reportId"]

    no_match_data = df_sorted_desc.reindex(columns=newidx)
    
    number_of_records = len(no_match_data)
    print(f"Total number of {file_type} files extracted today :",number_of_records)
       
    if os.path.exists(f"{script_path}\\daily_files\\{file_type}\\{prev_day}"):
     ##If the directory exists, delete it and then create it again
        shutil.rmtree(f"{script_path}\\daily_files\\{file_type}\\{prev_day}")
        
    os.makedirs(f"{script_path}\\daily_files\\{file_type}\\{prev_day}")
    
    # Convert 'dataStartTime'  and 'dataEndTime' to datetime
    no_match_data['dataStartTime'] = pd.to_datetime(no_match_data['dataStartTime'], utc=True)
    no_match_data['dataEndTime'] = pd.to_datetime(no_match_data['dataEndTime'], utc=True)

    # Format the datetime column to the desired format
    no_match_data['formattedTime'] = (no_match_data['dataStartTime'].dt.strftime('%Y%m%d%H%M%S') + '_' +no_match_data['dataEndTime'].dt.strftime('%Y%m%d%H%M%S'))
    
    retries = 15
    for idx, row in no_match_data.iterrows():
        formatted_time = row['formattedTime']
        report_id = row['reportDocumentId']
        
        # Create the filename
        filename = f"{script_path}\\daily_files\\{file_type}\\{prev_day}\\Amazon_{file_type}_Transaction_{prev_day}_{idx + 1}.csv"
        
        attempt = 0
        while attempt < retries:
            try:
                # Call the method to download the report
                reports.get_report_document(report_id, download=True, file=filename)
                print(f"Report {report_id} downloaded to {filename}")
                time.sleep(10)
                break  # Exit loop on success
            except Exception as e:
                # Check for quota exceeded error
                if 'QuotaExceeded' in str(e):
                    print(f"QuotaExceeded error for report {report_id}: {e}")
                    if attempt < retries - 1: 
                        wait_time = 60
                        attempt += 1 
                        print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed to download Amazon MPI {file_type} file after {retries} attempts.")
                        failure_subject = f"Failure - Amazon MPI transaction report daily files extraction failed for {prev_day}" 
                        failure_body = f"Script mpi_transaction_report_wrapper.py failed to download {file_type} transaction file after {retries} attempts for {prev_day}\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}\nTimeout time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        send_start_end_mail(failure_subject,failure_body)
                        break  # Exit loop if max retries are exhausted
                else:
                    print(f"Failed to download Amazon MPI {file_type} file: {e}")
                    failure_subject = f"Failure - Amazon MPI transaction report daily files extraction failed for {prev_day}" 
                    failure_body = f"Script mpi_transaction_report_wrapper.py failed to download {file_type} transaction file for {prev_day}\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}\nTimeout time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    send_start_end_mail(failure_subject,failure_body)
                    break
                    
    #call the function to modify the extracted files 
    combine_files(file_type,header)
    
    #call the function which executes amz_transaction_modification.py script
    file_modification(file_type)
    
def get_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def find_and_remove_duplicates(file_paths):
    seen_files = {}
    for file_path in file_paths:
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            file_hash = get_file_hash(file_path)
            # Check for duplicates
            if (file_size, file_hash) in seen_files:
                print(f"Duplicate found: {file_path} is a duplicate of {seen_files[(file_size, file_hash)]}.")
                os.remove(file_path)  # Delete the duplicate
                print(f"Deleted: {file_path}")
            else:
                seen_files[(file_size, file_hash)] = file_path

def read_file_to_list(filename):
    """Read a file and return a list of lines."""
    with open(filename, 'r', encoding='utf-8-sig') as file:
        return [line.strip() for line in file]

def combine_files(file_type, header):
    os.chdir(script_path)
    sys.stdout = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    
    daily_files_folder = f"{script_path}\\daily_files\\{file_type}\\{prev_day}\\"
    combined_folder = f"{script_path}\\daily_files\\{file_type}\\{prev_day}\\combined_file"
    file_list = [f for f in os.listdir(daily_files_folder) if f.endswith('.csv')]
    
    # Finding and removing duplicates
    find_and_remove_duplicates([os.path.join(daily_files_folder, f) for f in file_list])
    file_list_updated = [f for f in os.listdir(daily_files_folder) if f.endswith('.csv')] 
    
    #sort the data in desc order based on size
    file_paths = [os.path.join(daily_files_folder, f) for f in file_list_updated]
    file_paths.sort(key=os.path.getsize, reverse=True)    
    #all_files = [f for f in os.listdir(daily_files_folder) if os.path.isfile(os.path.join(daily_files_folder, f))]
    
    res = read_file_to_list(os.path.join(daily_files_folder, file_paths[0]))

    # Create a set to track unique entries in the first file (largest)
    seen = set(res)  # To track unique entries from the largest file
    
    # Iterate over each subsequent file
    for filename in file_paths[1:]:
        current_list = read_file_to_list(os.path.join(daily_files_folder, filename))
    
        for line in current_list[1:]:  # Skip the header
            if line not in seen:
                res.append(line)  # Add only if it's not already in the largest file
    
    # Write the result to a new file
    output_path = os.path.join(combined_folder, f'Amazon_{file_type}_Transaction_{prev_day}.csv')    
    os.makedirs(combined_folder, exist_ok=True)  
    with open(output_path, 'w', encoding='utf-8-sig') as outfile:   #file was of UTF-8-BOM type
        for item in res:
            outfile.write(item + '\n')
    
    print(f"Combined file created at: {output_path}")
            
def file_modification(file_type):
    os.chdir(script_path)
    sys.stdout = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    
    print(f"\ncoping data files from Desoto VM to target S3")
    subprocess.run(["aws", "s3", "cp", f"{script_path}/daily_files/{file_type}/{prev_day}/combined_file/", f"{tgt_s3}/delta_files/{file_type}/{prev_day}/",  "--recursive"], shell=True)
    time.sleep(2)
    
    #Call python script to modify Amazon MPI Transaction file and load into modified file folder
    log_extr = f"{log_dir}/amz_transaction_{file_type}_modification_{run_date}.log" 
    with open(log_extr, "w") as f:
        subprocess.run(["python",f"amz_transaction_modification.py", "-d", prev_day, "-c", f"amz_transaction_redshift.config","-t", file_type], stdout=f,stderr=f)
        time.sleep(2)

    while True:
        with open(log_extr, 'r') as file:
            log_file_ora_content = file.read()
    
        # Use regular expressions to find the word 'state : complete'
        match1 = re.search(r'state\s*:\s*complete', log_file_ora_content)
        
        if match1:
            print(f"\nAmazon transaction file modification script completed for {file_type} at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            break
        else:
            time.sleep(30)


    #Upload modified files to S3
    print(f"\nCopy modified Amazon transaction {file_type} file from Desoto VM to S3 started  at :", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    subprocess.run(["aws", "s3", "cp", f"{script_path}/modified_files/{prev_day}/{file_type}/", f"{tgt_s3}/Modified_files/{file_type}/{prev_day}","--exclude", "*","--include", "mod*", "--recursive"], shell=True)
    time.sleep(2)
    
    print(f"\nCopy modified Amazon transaction {file_type} file from Desoto VM to S3 completed  at :", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    #create a trigger file and upload it to s3
    with open(f"{trigger_dir}\\amz_transactions_{file_type}_{prev_day}.txt",'w') as file:
        pass
    
    time.sleep(2)
    
    subprocess.run(["aws", "s3", "cp", f"{trigger_dir}/amz_transactions_{file_type}_{prev_day}.txt", f"{trigger_s3}/"], shell=True)
    
    print(f"\nTrigger file has been generated for Amazon Transaction {file_type}" )

    email_completion_subject = f"Amazon MPI ({file_type}) Transactions files extraction completed for {prev_day}"
    email_completion_body = f"""Script mpi_transaction_report_wrapper.py for ({file_type}) Transactions files extraction completed for the date: {prev_day}.\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}\ncompletion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    send_start_end_mail(email_completion_subject,email_completion_body)    
	
                    
def main():
    os.chdir(script_path)
    sys.stdout = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\mpi_transaction_report_wrapper_{run_date}.log", 'a')
    
    print(f"***********************************************")
    print("Rundate is:", run_date)
    print("previous date is:", prev_day)
    
    print(f"Extracting data from :", prev_date_f)  
    print(f"Extracting data till :", run_date_f)
    
    #ATVPDKIKX0DER : MPI USA
    
    #A2EUQ1WTGCTBG2 : MPI CA
    
    #A1AM78C64UM0Y8 :  MPI MX
 
    ####BLOCK FOR USA FILE####    
    us_mark_id = ["ATVPDKIKX0DER"]
    us_file_type = "USA"
    header_us = ['date/time', 'settlement id', 'type', 'order id', 'sku', 'description','quantity', 'marketplace', 'account type', 'fulfillment', 'order city', 'order state','order postal' , 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other', 'total']
    
    ####send start mail for USA####
    us_start_mail_subject = f"Amazon MPI ({us_file_type}) Transactions files extraction started for {prev_day}"
    us_start_body = f"""Script mpi_transaction_report_wrapper.py for ({us_file_type}) Transactions files extraction started for: {prev_day}\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}"""
    send_start_end_mail(us_start_mail_subject,us_start_body)
    
    ####extract and download US file####
    print(f"\nAmazon MPI USA Transactions file extraction started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    extract_and_download_file_through_api(us_mark_id,us_file_type,header_us)
    
    ####BLOCK FOR CA FILE#### 
    ca_mark_id = ["A2EUQ1WTGCTBG2"]
    ca_file_type = "CA"
    header_ca = ['date/time', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'gift wrap credits tax', 'Regulatory fee', 'Tax on regulatory fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other','total']
    
    ####send start mail for CA####
    ca_start_mail_subject = f"Amazon MPI ({ca_file_type}) Transactions files extraction started for {prev_day}"
    ca_subject_body = f"""Script mpi_transaction_report_wrapper.py for ({ca_file_type}) Transactions files extraction started for: {prev_day}\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}"""
    send_start_end_mail(ca_start_mail_subject,ca_subject_body)
    
    #######extract and download CA file####
    print(f"***********************************************") 
    print(f"\nAmazon MPI CA Transactions file extraction started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    extract_and_download_file_through_api(ca_mark_id,ca_file_type,header_ca) 
    
    ####BLOCK FOR MX FILE#### 
    mx_mark_id = ["A1AM78C64UM0Y8"]
    mx_file_type = "Mexico"
    header_mx = ["fecha/hora","Id. de liquidación","tipo","Id. del pedido","sku","descripción","cantidad","marketplace","cumplimiento","ciudad del pedido","estado del pedido","código postal del pedido","modelo de recaudación de impuestos","ventas de productos","impuesto de ventas de productos","créditos de envío","impuesto de abono de envío","créditos por envoltorio de regalo","impuesto de créditos de envoltura","Tarifa reglamentaria","Impuesto sobre tarifa reglamentaria","descuentos promocionales","impuesto de reembolsos promocionales","impuesto de retenciones en la plataforma","tarifas de venta","tarifas fba","tarifas de otra transacción","otro","total"]
    
    ####send start mail for MX####
    mx_start_mail_subject = f"Amazon MPI ({mx_file_type}) Transactions files extraction started for {prev_day}"
    mx_subject_body = f"""Script mpi_transaction_report_wrapper.py for ({mx_file_type}) Transactions files extraction started for: {prev_day}\nThis script is scheduled on cbiankitw10.global.local VM.\n\nStart time : {start_time}"""
    send_start_end_mail(mx_start_mail_subject,mx_subject_body)
    
    ####extract and download MX file####
    print(f"***********************************************") 
    print(f"\nAmazon MPI Mexico Transactions file extraction started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    extract_and_download_file_through_api(mx_mark_id,mx_file_type,header_mx)
    create_script_status_file("mpi_transaction_report_wrapper","daily","Marketplace","cbiankitw10",start_time,datetime.now().strftime('%Y%m%d%H%M%S'))
    
if __name__ == "__main__":
    main()









