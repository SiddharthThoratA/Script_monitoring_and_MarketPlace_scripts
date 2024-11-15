#   NAME: settlement_report_wrapper.sh       
#   DATE: 10-09-2024      
#   DESCRIPTION: Extract data from SP-API unload files to S3 and load data in redshift table. 
# ==========================================================================================================================================
# DATE       	MODIFIED BY        DESCRIPTION
# 12-sept-2024	Kartik Parate	   Created
# 22-Oct-2024	Kartik Parate	   Added report_type and posted_date_time_pst field.
#
# ==========================================================================================================================================

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
from sp_api.base import Marketplaces
import shutil
import subprocess
from dateutil import parser
import random
from pytz import timezone, UnknownTimeZoneError
from datetime import datetime, timezone
import pytz
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *


run_date = datetime.now().strftime('%Y%m%d')
#run_date ='20241020'
run_date_f = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
previous_day_f = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
script_path = "D://MPI//settlement_report//"
log_dir = f"{script_path}//log"
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
log_file_email = f"{log_dir}\email_notication_{run_date}.log"
redshift_config = "redshift_conn.config"


listOfGlobals = globals()
listOfGlobals['log_dir'] = log_dir
listOfGlobals['script_path'] = script_path
listOfGlobals['start_time'] = start_time
listOfGlobals['previous_day_f'] = previous_day_f
listOfGlobals['run_date'] = run_date
listOfGlobals['run_date_f'] = run_date_f
listOfGlobals['redshift_config'] = redshift_config

os.environ["ENV_DISABLE_DONATION_MSG"] = "1"



def start_and_completion_mail(subject,body):
    log_file_email = f"{log_dir}\email_notication_{run_date}.log"
    config_file = "email_config.config"
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}/email_notification.py", "--s",subject ,"--b",body, "--c", config_file], stdout=f)

    

def read_json_file():
    
    # Write the JSON object to a file
    json_path = f'{script_path}//credentials.json'
    
    with open(json_path) as json_file:
        json_data = json.load(json_file)
        
    
    refresh_token = json_data['refresh_token']
    lwa_app_id = json_data['lwa_app_id']
    lwa_client_secret = json_data['lwa_client_secret']
    
    return refresh_token,lwa_app_id,lwa_client_secret
    
        
def extract_and_download_file_through_api(output_file_name):
    sys.stdout = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    credentials_details=read_json_file()
    
    #credentials details(client_id,secret and tocken number)
    credentials = dict(
    refresh_token = credentials_details[0],
    lwa_app_id = credentials_details[1],
    lwa_client_secret = credentials_details[2],
    )
    
    #Pass the credentials and create report client to connect SPI-API
    reports=Reports(credentials=credentials)
    
    #Create a list of report types which need to generated
    report_types = ["GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"]
    
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
                params['createdSince'] = f"{previous_day_f}T00:00:01+00:00"
                params['createdUntil'] = f"{run_date_f}T00:00:00+00:00"
                params['pageSize'] = 100 
            
            print("parameter is",params) 
            # Make the API call
            res = reports.get_reports(**params)

            reports_list = getattr(res, 'reports', [])
            all_reports.extend(reports_list)
            
            next_token = res.next_token
            
            print("next token is :",next_token)
            time.sleep(60)
            # Break the loop if there are no more pages
            if not next_token:
                print("not getting token")
                break
                
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    
    
    df = pd.DataFrame(all_reports)
    
    
    if df.empty:
        print("No new files found so sending mail and exiting from here.....")
        print(f"****************************##############################****************************")
        subject = f"Amazon MPI settlement report- no new report available for {run_date}"
        body = f"Today we haven't received new settlement report for {run_date}"
        start_and_completion_mail(subject,body)
        
        # generating trigger file 
        with open(f"{script_path}//trigger_files//trigger_settlement_report_{run_date}.txt",'w'):
            pass
        
    
        trigger_s3_path = "s3://desototech/Marketplace/API_Extraction/settlement_reports/trigger_files"
        subprocess.run(["aws", "s3", "cp", f"{script_path}//trigger_files//trigger_settlement_report_{run_date}.txt", f"{trigger_s3_path}/"], shell=True)
        create_script_status_file("settlement_report_wrapper","daily","Marketplace","cbiankitw10",start_time,datetime.now().strftime('%Y%m%d%H%M%S'))
        exit()
    else:
        print("Found some files to download, so processing further...")
    
    df['processingEndTime'] = pd.to_datetime(df['processingEndTime'])
    
    # Sorted records according to processingEndTime
    df_sorted_desc = df.sort_values(by='processingEndTime', ascending=False)
    # reindex the df
    newidx = ["reportType","processingStartTime","processingEndTime","dataStartTime","dataEndTime","createdTime","processingStatus","marketplaceIds","reportDocumentId","reportId"]

    no_match_data = df_sorted_desc.reindex(columns=newidx)
    
    number_of_records = len(no_match_data)
    print("Total number of files extracted today :",number_of_records)
       
    if os.path.exists(f'{output_file_name}\\{run_date}'):
    # If the directory exists, delete it
        shutil.rmtree(f'{output_file_name}\\{run_date}')
    
    os.makedirs(f'{output_file_name}\\{run_date}')
    
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
        filename = os.path.join(output_file_name, run_date, f'settlement_report_{formatted_time}_{idx + 1}.csv')
        
        attempt = 0
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
                        failure_subject = f"Alert! Failure- Amazon MPI settlement report daily files extraction for {run_date}" 
                        failure_body = f"Failed to download report {report_id} after {retries} attempts for {run_date}."
                        start_and_completion_mail(failure_subject,failure_body)
                        break  # Exit loop if max retries are exhausted
                else:
                    print(f"Failed to download report {report_id}: {e}")
                    failure_subject = f"Alert! Failure- Amazon MPI settlement report daily files extraction for {run_date}" 
                    failure_body = f"Failed to download report {report_id} for {run_date}."
                    start_and_completion_mail(failure_subject,failure_body)
                    break  # Exit loop if error is not related to quota
                 
    
    return number_of_records

def convert_timezone(date):
    try:
        # Check if the input is a valid datetime
        if not isinstance(date, datetime):
            raise ValueError("Input must be a datetime object.")
        
        # Set original timezone to UTC
        original_timezone = timezone.utc
        target_timezone = pytz.timezone('US/Pacific')

        # If the date has no tzinfo, assume it's in UTC
        if date.tzinfo is None:
            date = date.replace(tzinfo=original_timezone)
        else:
            # Ensure the date is treated as UTC if it has a different tzinfo
            date = date.astimezone(original_timezone)

        # Convert to target timezone (PST)
        pst_date = date.astimezone(target_timezone)

        # Return the date formatted as a string without timezone
        return pst_date.strftime('%Y-%m-%d %H:%M:%S')
    
    except ValueError as ve:
        print(f"ValueError: {ve}")
        return None  # or handle the error as needed
    except pytz.UnknownTimeZoneError:
        return date.strftime('%Y-%m-%d %H:%M:%S')
    

def file_modification(output_file_name,mod_output_file_name,report = 'Marketplace Impact'):
    sys.stdout = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    input_dir = f"{output_file_name}\\{run_date}"
    output_dir = f"{mod_output_file_name}\\{run_date}"
    
    if os.path.exists(output_dir):
    # If the directory exists, delete it
        shutil.rmtree(output_dir)
    
    os.makedirs(output_dir)
        
    date_columns = ['settlement-start-date', 'settlement-end-date','deposit-date','posted-date-time','posted-date']
    replace_value = {' UTC': ''}
    integer_columns = ['order-item-code','quantity-purchased']
    date_formats = ['%d-%m-%Y %H:%M:%S', '%m-%d-%Y %H:%M:%S' , '%Y-%m-%d %H:%M:%S', '%Y-%d-%m %H:%M:%S','%Y-%m-%d','%d-%m-%Y']
    final_cols =['settlement-id', 'settlement-start-date', 'settlement-end-date', 'deposit-date', 'total-amount', 'currency', 'transaction-type', 'order-id', 'merchant-order-id', 'adjustment-id', 'shipment-id', 'marketplace-name', 'amount-type', 'amount-description', 'amount', 'fulfillment-id', 'posted-date', 'original_posted_date_time', 'order-item-code', 'merchant-order-item-id', 'merchant-adjustment-item-id', 'sku', 'quantity-purchased', 'promotion-id', 'report_type', 'run_date', 'filename', 'posted_date_time_pst' ]
    float_column = ['amount','total-amount']
    
    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name,)
    
        if file_name.endswith('.csv'): 
    
            df = pd.read_csv(file_path, delimiter = '\t',low_memory=False)
            df['report_type'] = report
            df['run_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['filename'] = file_name
            df['posted_date_time_pst'] = ''
        
            for col in integer_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            for col in float_column:
                df[col] = df[col].astype(str).str.replace(',', '.', regex=True).astype(float)
        
            df[date_columns] = df[date_columns].replace(replace_value, regex=True)
        
            for col in date_columns:
                df[col] = df[col].str.replace(r'[./]','-',regex=True)
        
            for col in date_columns:
                for fmt in date_formats:
                    try:
                        df[col] = pd.to_datetime(df[col], format=fmt, errors='raise')
                        break 
                    except (ValueError, TypeError):
                        pass 
            
            for col in date_columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
            new_df = df.copy()
            
            df['posted-date-time'] = df['posted-date-time'].astype(str).str.replace('\u200B', '').str.strip()
            df['posted-date-time'] = pd.to_datetime(df['posted-date-time'], errors='coerce')
                    
            df['original_posted_date_time'] = new_df['posted-date-time']
            df['posted_date_time_pst'] = df['posted-date-time'].apply(convert_timezone)
    
            df = df.drop(columns=['posted-date-time'],axis=1)
            df = df.reindex(columns=final_cols)

        
            value_col2 = df.iloc[0, 1]
            value_col3 = df.iloc[0, 2]
            value_col4 = df.iloc[0, 3]
            value_col5 = df.iloc[0, 4]
            value_col6 = df.iloc[0, 5]
            
            df.iloc[:, 1] = value_col2
            df.iloc[:, 2] = value_col3
            df.iloc[:, 3] = value_col4
            df.iloc[:, 4] = value_col5
            df.iloc[:, 5] = value_col6
            
    
            output_file_path = os.path.join(output_dir, f"mod_{file_name}")
        
            df.to_csv(output_file_path, index=False)
            print(f'Processed and saved: {output_file_path}')


def upload_file_on_s3(output_file_name,mod_output_file_name,mod_s3_path_data,s3_path_data):
    #Uploading src_file on s3
    subprocess.run(["aws", "s3", "cp", f"{output_file_name}/{run_date}/", f"{s3_path_data}/{run_date}/",  "--recursive"], shell=True)
    # uploading modified file on s3
    subprocess.run(["aws", "s3", "cp", f"{mod_output_file_name}/{run_date}/", f"{mod_s3_path_data}/{run_date}/",  "--recursive"], shell=True)
    
            
            
            
            
def main():
    
    os.chdir(script_path)
    
    with open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log",'w') as f:
        pass
    
    sys.stdout = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    sys.stderr = open(f"{log_dir}\\settlement_report_wrapper_{run_date}.log", 'a')
    
    
    print(f"**********************************************************************************************")
    print(f"run date is : {run_date}")
    print(f"script path is : {script_path}")
    print(f"script start time : ", start_time)
    
    
    print(f"**********************************************************************************************")
    print(f"Sending start mail..")
    #Start mail
    start_mail_subject = f'Amazon MPI settlement report daily files extraction started for {run_date}'
    start_mail_body = f'Job settlement_report_wrapper.py started for {run_date}\n\nStart time : {start_time}'
    start_and_completion_mail(start_mail_subject,start_mail_body)
    print(f"start mail has been send sucessfully...")
    print(f"**********************************************************************************************")
    
    #Calling function to extract and download file trough API
    print(f"Calling extract_and_download_file_through_api function to extract and download file trough API")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    
    output_file_name = f"D://MPI//settlement_report//data_files"
    number_of_records = extract_and_download_file_through_api(output_file_name)
    
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function extract_and_download_file_through_api  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    print(f"function file_modification  has been called..")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    
    mod_output_file_name = f"D://MPI//settlement_report//mod_data_files"
    file_modification(output_file_name,mod_output_file_name)
    
    
    print(f"function completed time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"function file_modification  has been completed sucessfully.")
    print(f"**********************************************************************************************")
    
    print(f"function upload_file_on_s3  has been called..")
    print(f"function start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    s3_path_data = "s3://desototech/Marketplace/API_Extraction/settlement_reports/src_data_files"
    mod_s3_path_data = "s3://desototech/Marketplace/API_Extraction/settlement_reports/mod_data_files"
    upload_file_on_s3(output_file_name,mod_output_file_name,mod_s3_path_data,s3_path_data)
    
    
    
    print(f"function upload_file_on_s3  has been completed..")
    print(f"function completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f"**********************************************************************************************")
    
    # generating trigger file 
    with open(f"{script_path}//trigger_files//trigger_settlement_report_{run_date}.txt",'w'):
        pass
        
    trigger_s3_path = "s3://desototech/Marketplace/API_Extraction/settlement_reports/trigger_files"
    subprocess.run(["aws", "s3", "cp", f"{script_path}//trigger_files//trigger_settlement_report_{run_date}.txt", f"{trigger_s3_path}/"], shell=True)
    
    
    #Completion mail
    completion_subject = f"Amazon MPI settlement report daily files extraction completed for {run_date}"
    completion_body = f"Job settlement_report_wrapper.py completed for {run_date}\n\nCount of new files downloaded : {number_of_records}\n\nStart time : {start_time}\n\nCompletion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    start_and_completion_mail(completion_subject,completion_body)
    print(f"completion mail has been send sucessfully...")
    print(f"**********************************************************************************************")
    print(f"Script has been completed sucessfully.")
    create_script_status_file("settlement_report_wrapper","daily","Marketplace","cbiankitw10",start_time,datetime.now().strftime('%Y%m%d%H%M%S'))


# Calling main function
if __name__ == "__main__":
    main()