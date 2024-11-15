#   NAME: amazon_mpi_inventory_wrapper.py     
#   DATE: 30-09-2024      
#   DESCRIPTION: Extract data from SP-API and modify files and unload files to S3 . 
# ==============================================================================
# DATE       	MODIFIED BY        DESCRIPTION
# 30-sept-2024	Kartik Parate	   Created
# 18-oct-2024   Kartik Parate      Added create report logic.
# ==============================================================================

import sys, getopt
from datetime import datetime, timedelta
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.api import Reports
import pandas as pd
import json
import os
import time
import glob
import shutil
import subprocess
from dateutil import parser
import random
import csv
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *


# Defining Variables
run_date = datetime.now().strftime('%Y%m%d')
current_date_f = datetime.now().strftime('%Y-%m-%d')
next_day_f = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
previous_day_f = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
#previous_day_f = '2024-09-01'
previous_day_f_formated = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
#previous_day_f_formated = '20240901'
script_path = "D://MPI//Inventory"
log_dir = f"{script_path}//log"
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
s3_path = "s3://desototech/Marketplace/Amazon/amazon_inventory"
#s3_path = "s3://desototech/DWH_team/Kartik/Marketplace/Amazon/amazon_inventory"


#Defining common variables as global variable to use in functions
listOfGlobals = globals()
listOfGlobals['log_dir'] = log_dir
listOfGlobals['script_path'] = script_path
listOfGlobals['start_time'] = start_time
listOfGlobals['previous_day_f'] = previous_day_f
listOfGlobals['run_date'] = run_date
listOfGlobals['current_date_f'] = current_date_f
listOfGlobals['next_day_f'] = next_day_f
listOfGlobals['previous_day_f_formated'] = previous_day_f_formated
listOfGlobals['s3_path'] = s3_path


# enviorment variable to disable the donation message from SP-API.
os.environ["ENV_DISABLE_DONATION_MSG"] = "1"


# Function which send strat,completion or failure mail.
def start_and_completion_mail(subject,body):
    log_file_email = f"{log_dir}\email_notication_{run_date}.log"
    config_file = "email_config.config"
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}/email_notification.py", "--s",subject ,"--b",body, "--c", config_file], stdout=f)

    
# function which reads SP-API Credentials from file.
def read_json_file():
    
    # Write the JSON object to a file
    json_path = f'{script_path}//credentials.json'
    
    with open(json_path) as json_file:
        json_data = json.load(json_file)
        
    
    refresh_token = json_data['refresh_token']
    lwa_app_id = json_data['lwa_app_id']
    lwa_client_secret = json_data['lwa_client_secret']
    
    return refresh_token,lwa_app_id,lwa_client_secret
    
# function to create report file 
def create_report(credentials):
    
    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    
    #Optional Parameter
    report_option = dict(
        aggregatedByTimePeriod ="DAILY",
        aggregateByLocation = "FC"
        )
    try:
        Reports(credentials=credentials).create_report(
            reportType=ReportType.GET_LEDGER_SUMMARY_VIEW_DATA,
            dataStartTime = f"{previous_day_f}T00:00:00+00:00",
            dataEndTime = f"{previous_day_f}T23:59:59+00:00",
            marketplaceIds=[
                "ATVPDKIKX0DER"
            ],
            reportOptions = report_option )
    except Exception as e:
        print(f"unable to create report : {e}")
        failure_subject = f"Alert! Failure- Amazon MPI Inventory daily files extraction for {run_date}" 
        failure_body = f'Failed to create Amazon MPI Inventory daily file for {run_date}.\nUnable to create report : {e}\nThis script is scheduled on "cbiankitw10.global.local" VM.'
        start_and_completion_mail(failure_subject,failure_body)
        exit()
    time.sleep(120) # Wait for report to generate
    

# Function which dynamically detects the delimiter of file.    
def detect_delimiter(file_path):
    with open(file_path, 'r') as file:
        sample = file.read(5012) # Read a sample of the file
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
        return delimiter
    
     
def extract_reportDocumentId(credentials):

    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    
    #Pass the credentials and create report client to connect SPI-API
    reports=Reports(credentials=credentials)
    
    #Create a list of report types which need to generated
    report_types = ["GET_LEDGER_SUMMARY_VIEW_DATA"]
    
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
                params['createdSince'] = f"{current_date_f}T00:00:01+00:00"
                params['createdUntil'] = f"{next_day_f}T00:00:00+00:00"
                params['pageSize'] = 100 
                params['marketplaceIds'] = ["ATVPDKIKX0DER"] # For MPI USA
            
            print("parameter is",params) 
            # Makeing the API call
            res = reports.get_reports(**params)

            reports_list = getattr(res, 'reports', [])
            all_reports.extend(reports_list)
            
            next_token = res.next_token
            
            print("next token is :",next_token)
            time.sleep(10)
            # Break the loop if there are no more pages
            if not next_token:
                print("not getting token")
                break
                
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    
    
    df = pd.DataFrame(all_reports)
    
    # Converting string filed to Datetime
    df['dataStartTime'] = pd.to_datetime(df['dataStartTime'])
    df['dataEndTime'] = pd.to_datetime(df['dataEndTime'])
    
    #extracting date from filed and creatting new fields in df to filter the data.
    df['dataStartTime_date'] = df['dataStartTime'].dt.date
    df['dataEndTime_date'] = df['dataEndTime'].dt.date
    
    filter_date =  pd.to_datetime(f'{previous_day_f}').date()
    filter_status = 'DONE'
    
    print(f"Data frame filters are : \nFilter = {filter_date}\nstatus_filter ={filter_status}")
    print("\nGoing to apply filter")
    # Filtering data on dataStartTime_date and dataEndTime_date and status
    filtered_df = df[(df['dataStartTime_date'] == filter_date) & (df['dataEndTime_date'] == filter_date) & (df['processingStatus'] == filter_status)]
    
    #Creating new df
    newdf = filtered_df.copy()
    
    if newdf.empty:
        print("still file not genrated")
        return None
    else:
        print("file_genearted")
        no_match_data = newdf.iloc[[0]]
    
        for idx,row in no_match_data.iterrows():
            report_id = row['reportDocumentId']
            id1 = row['reportId']
            print("we are getting report ",id1)
            
    return report_id
           
    
# function which extract and download file     
def extract_and_download_file_through_api(output_file_name,report_id,credentials):
    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    
    #Pass the credentials and create report client to connect SPI-API
    reports=Reports(credentials=credentials)
    
    #Create a list of report types which need to generated
    report_types = ["GET_LEDGER_SUMMARY_VIEW_DATA"]
   
    if os.path.exists(f'{output_file_name}\\{previous_day_f_formated}'):
    # If the directory exists, delete it
        shutil.rmtree(f'{output_file_name}\\{previous_day_f_formated}')
    
    os.makedirs(f'{output_file_name}\\{previous_day_f_formated}')
   
    
    retries = 15
    attempt = 0
   
    # Create the filename
    filename = os.path.join(output_file_name, previous_day_f_formated, f'Amazon_US_Inventory_{previous_day_f_formated}.csv')
    
    while attempt < retries:
        try:
            # Call the method to download the report
            reports.get_report_document(report_id, download=True, file=filename)
            print(f"Report {report_id} downloaded to {filename}")
            time.sleep(10)
            break  # Exit loop on success
        except Exception as e:
            # Check for specific quota exceeded error
            if 'QuotaExceeded' in str(e):
                print(f"QuotaExceeded error for report {report_id}: {e}")
                if attempt < retries - 1: 
                    wait_time = 60
                    attempt += 1 
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed to download report {report_id} after {retries} attempts.")
                    failure_subject = f"Alert! Failure- Amazon MPI Inventory daily files extraction for {run_date}" 
                    failure_body = f'Failed to download Amazon MPI Inventory daily file {report_id} after {retries} attempts for {run_date}.\nThis script is scheduled on "cbiankitw10.global.local" VM.'
                    start_and_completion_mail(failure_subject,failure_body)
                    exit()  # Exit loop if max retries are exhausted
            else:
                print(f"Failed to download report {report_id}: {e}")
                failure_subject = f"Alert! Failure- Amazon MPI Inventory daily files extraction for {run_date}" 
                failure_body = f'Failed to download Amazon MPI Inventory daily file {report_id} for {run_date}.\nThis script is scheduled on "cbiankitw10.global.local" VM.'
                start_and_completion_mail(failure_subject,failure_body)
                exit()  # Exit loop if error is not related to quota
                
                    
def parse_date(date_str):
    try:
        return parser.parse(date_str).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        print("error")
        return None    

def file_modification(output_file_name,mod_output_file_name):
    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    input_dir = f"{output_file_name}\\{previous_day_f_formated}"
    output_dir = f"{mod_output_file_name}\\{previous_day_f_formated}"
    
    if os.path.exists(output_dir):
    # If the directory exists, delete it
        shutil.rmtree(output_dir)
    
    os.makedirs(output_dir)
    
    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name,)
    
        if file_name.endswith('.csv'):
            
            try:
                file_delimiter = detect_delimiter(file_path)
                print(f"File: {file_name}, Detected Delimiter: {file_delimiter}")
            except Exception as e:
                print(f"Could not detect delimiter for {file_name}: {e}")
    
            df = pd.read_csv(file_path, delimiter = f'{file_delimiter}',low_memory=False)
            
            if df.empty:
                print(f"There is no data in file.")
                failure_subject = f"Alert! Failure- Amazon MPI Inventory daily files extraction for {run_date}" 
                failure_body = f'Failed- File has been downloaded but there is no data in file for {run_date}.\nThis script is scheduled on "cbiankitw10.global.local" VM.'
                start_and_completion_mail(failure_subject,failure_body)
                exit()
            
            df['file_name'] = file_name
            
            date_columns = ['Date']
            
            for col in date_columns:
                df[col] = df[col].str.replace(r'[./]','-',regex=True)
           
           
            # Apply the date parsing function to the date column
            for col in date_columns:
                df[col] = df[col].apply(parse_date)
            
            #Taking only required column if extra columns comes it will remove automatically.
            #Final columns
            final_cols = ['Date', 'FNSKU', 'ASIN', 'MSKU', 'Title', 'Disposition', 'Starting Warehouse Balance', 'In Transit Between Warehouses', 'Receipts', 'Customer Shipments', 'Customer Returns', 'Vendor Returns', 'Warehouse Transfer In/Out', 'Found', 'Lost', 'Damaged', 'Disposed', 'Other Events', 'Ending Warehouse Balance', 'Unknown Events', 'Location',  'file_name']
        
            df = df.reindex(columns=final_cols)
            
            print("Date format has been changed.")
            output_file_path = os.path.join(output_dir, f"mod_{file_name}")
        
            df.to_csv(output_file_path, sep=',', index=False)
            print(f'Processed and saved: {output_file_path}')

def upload_file_on_s3(output_file_name,mod_output_file_name,mod_s3_path_data,s3_path_data):

    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    #Uploading src_file on s3
    subprocess.run(["aws", "s3", "cp", f"{output_file_name}/{previous_day_f_formated}/", f"{s3_path_data}/{previous_day_f_formated}/",  "--recursive"], shell=True)
    # uploading modified file on s3
    subprocess.run(["aws", "s3", "cp", f"{mod_output_file_name}/{previous_day_f_formated}/", f"{mod_s3_path_data}/{previous_day_f_formated}/",  "--recursive"], shell=True)
    
            
  
def main():
    
    os.chdir(script_path)
    
    with open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log",'w') as f:
        pass
    
    sys.stdout = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\amazon_mpi_inventory_wrapper_{run_date}.log", 'a')
    
    
    print(f"**********************************************************************************************")
    print(f"run date is : {run_date}")
    print(f"script path is : {script_path}")
    print(f"script start time : ", start_time)
    print('\n')
    
    print(f"**********************************************************************************************")
    print(f"Sending start mail..")
    #Start mail
    start_mail_subject = f'Amazon MPI Inventory daily files extraction started for {run_date}'
    start_mail_body = f'Job amazon_mpi_inventory_wrapper.py started for {run_date}\nThis script is scheduled on "cbiankitw10.global.local" VM.\n\nStart time : {start_time}'
    start_and_completion_mail(start_mail_subject,start_mail_body)
    print(f"start mail has been send sucessfully...")
    print(f"**********************************************************************************************")
    print('\n')
    
    # read credentials from file.
    credentials_details=read_json_file()
    
    #credentials details(client_id,secret and tocken number)
    credentials = dict(
    refresh_token = credentials_details[0],
    lwa_app_id = credentials_details[1],
    lwa_client_secret = credentials_details[2],
    )
    
    
    #Calling function to Create previous day report file trough API
    print(f"Calling create_report function to extract and download file trough API")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    create_report(credentials)
    
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function create_report  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    
    #Calling function to get report document_id
    print(f"Calling extract_reportDocumentId function to extract and download file trough API")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    counter = 0
    timeout = 60
    
    report_id = None
    
    while counter < timeout :
        print("calling function extract_reportDocumentId")
        report_id = extract_reportDocumentId(credentials)
        print("report id is :",report_id)
        if report_id is None or report_id == 'NaN':
            print("still haven't received the file.")
            time.sleep(60)
            counter += 1
        else:
            print("Hurrey got the document_id\ndocument_id is", report_id)
            break
    else:
        print("after one hour of wait still document is not ready to download.")
        print(f"****************************##############################****************************")
        subject = f"Amazon MPI inventory - no new files available for {run_date}"
        body = f'Today we havent received new Amazon MPI inventory file for {run_date}\nThis script is scheduled on "cbiankitw10.global.local" VM.'
        start_and_completion_mail(subject,body)
        
        if os.path.exists(f"{script_path}//USA//mod_data_files//{previous_day_f_formated}"):
        # If the directory exists, delete it
            shutil.rmtree(f"{script_path}//USA//mod_data_files//{previous_day_f_formated}")
    
        os.makedirs(f"{script_path}//USA//mod_data_files//{previous_day_f_formated}")
        
        #Generating Blank file 
        with open(f"{script_path}//USA//mod_data_files//{previous_day_f_formated}//blank_file_{previous_day_f_formated}.csv",'w'):
            pass
        
        # generating trigger file 
        with open(f"{script_path}//trigger_files//trigger_MPI_{run_date}.txt",'w'):
            pass
        subprocess.run(["aws", "s3", "cp", f"{script_path}//USA//mod_data_files//{previous_day_f_formated}//blank_file_{previous_day_f_formated}.csv", f"{s3_path}/USA/Modified_files/{previous_day_f_formated}/"], shell=True)
        subprocess.run(["aws", "s3", "cp", f"{script_path}//trigger_files//trigger_MPI_{run_date}.txt", f"{s3_path}/trigger_files/"], shell=True)
        exit()
    
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function extract_reportDocumentId  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    
    
    #Calling function to extract and download file trough API
    print(f"Calling extract_and_download_file_through_api function to extract and download file trough API")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    
    output_file_name = f"{script_path}//USA//src_data_files"
    extract_and_download_file_through_api(output_file_name,report_id,credentials)
    
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function extract_and_download_file_through_api  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    print('\n')
    print(f"function file_modification  has been called..")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    
    mod_output_file_name = f"{script_path}//USA//mod_data_files"
    file_modification(output_file_name,mod_output_file_name)
    
    
    print(f"function completed time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function file_modification  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    print('\n')
    
    print(f"function upload_file_on_s3  has been called..")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    s3_path_data = f"{s3_path}/USA/Src_files"
    mod_s3_path_data = f"{s3_path}/USA/Modified_files"
    upload_file_on_s3(output_file_name,mod_output_file_name,mod_s3_path_data,s3_path_data)
    
    
    
    print(f"function upload_file_on_s3  has been completed..")
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"**********************************************************************************************")
    print('\n')
    
    # generating trigger file 
    with open(f"{script_path}//trigger_files//trigger_MPI_{run_date}.txt",'w'):
        pass
        
    trigger_s3_path =  f"{s3_path}/trigger_files"
    subprocess.run(["aws", "s3", "cp", f"{script_path}//trigger_files//trigger_MPI_{run_date}.txt", f"{trigger_s3_path}/"], shell=True)
    
    
    #Completion mail
    completion_subject = f"Amazon MPI Inventory daily files extraction completed for {run_date}"
    completion_body = f'Job amazon_mpi_inventory_wrapper.py completed for: {run_date}.\nThis script is scheduled on "cbiankitw10.global.local" VM.\n\nStart time : {start_time}\nCompletion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'
    start_and_completion_mail(completion_subject,completion_body)
    print(f"completion mail has been send sucessfully...")
    print(f"**********************************************************************************************")
    print('\n')
    print(f"Script has been completed sucessfully.")
    create_script_status_file("amazon_mpi_inventory_wrapper","daily","Marketplace","cbiankitw10",start_time,datetime.now().strftime('%Y%m%d%H%M%S'))


# Calling main function
if __name__ == "__main__":
    main()