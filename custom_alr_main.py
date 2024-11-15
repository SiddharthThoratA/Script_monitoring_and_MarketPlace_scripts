from datetime import datetime, timedelta
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.api import Reports
import pandas as pd
import json
from s3_uploader import *
import os
import subprocess
import sys
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *


"""
NAME: custom_alr_main.py
CREATED DATE: 05-Jun-2024
Created By : Siddharth Thorat
DESCRIPTION: This script download Custom ALL Listing report from Amazon using SP-API and 
             also modified this report as per requirement of Marketplace Team
================================================================================================================
"""

script_path = r"D:\MPI\custom_alr"
raw_path = r"D:\MPI\custom_alr\daily_files\raw_data_reports"
mod_path = r"D:\MPI\custom_alr\daily_files\modified_reports"
trans_path = r"D:\MPI\custom_alr\daily_files\transformed_reports"
bucket = 'desototech'
s3_path = 'Marketplace/API_Extraction/MPI_reports/custom_alr'
log_dir = r"D:\MPI\custom_alr\logs"
run_dt = datetime.now().strftime('%Y%m%d')
starttime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


sys.stdout = open(f"{log_dir}\\custom_alr_{run_dt}.log", 'a')

os.chdir(script_path)

log_file_email = f"{log_dir}\\email_notication_{run_dt}.log"



def script_start_notification():

    email_start_subject = f"MPI Custom ALR weekly report extraction started for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
Job custom_alr_main.py for extraction of weekly Custom ALR has started for {run_dt}

Start time : {starttime}
"""
    #print(email_start_body)
    config_file = "email_config.config"

    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
def script_competion_notification():
    
    email_completion_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    email_start_subject = f"MPI Custom ALR weekly report extraction completed for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
Job custom_alr_main.py for extraction of weekly Custom ALR has completed for {run_dt}

Start time : {starttime}
Completion time : {email_completion_time}
"""
    #print(email_start_body)
    config_file = "email_config.config"

    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)


def create_folder():
    #Get current date
    today = datetime.now().strftime("%Y%m%d")
    print('---------------------------------------------------')
    print("Current date is: ",today)
    
    #out_file_path = f'{ofile_path}\\{today}'
    glob_folders = []
    
    folders_list = [raw_path, mod_path, trans_path]
    
    for folder in folders_list:
        run_date_folder = os.path.join(folder,today)
        if not os.path.exists(run_date_folder):
            os.mkdir(run_date_folder)
            print('---------------------------------------------------')
            print("Folder is created:",run_date_folder)
            glob_folders.append(run_date_folder)
            
        else:
            print('---------------------------------------------------')
            print(f'Path is already exists: ',run_date_folder)
            glob_folders.append(run_date_folder)
    
    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_date'] = today
    listOfGlobals['raw_file_path'] = glob_folders[0]
    listOfGlobals['mod_file_path'] = glob_folders[1]
    listOfGlobals['trans_file_path'] = glob_folders[2]
    
    print('---------------------------------------------------')
    print('Raw file path is:',raw_file_path)
    print('---------------------------------------------------')
    print('Modified file path is:',mod_file_path)
    print('---------------------------------------------------')
    print('transformed file path is:',trans_file_path)
    

def read_json_file():
    
    # Write the JSON object to a file
    json_path = r'D:\MPI\custom_alr\credentials.json'
    
    with open(json_path) as json_file:
        json_data = json.load(json_file)
        
    
    refresh_token = json_data['refresh_token']
    lwa_app_id = json_data['lwa_app_id']
    lwa_client_secret = json_data['lwa_client_secret']
    
    return refresh_token,lwa_app_id,lwa_client_secret


def generate_custom_alr():
    
    #send start email
    script_start_notification()
    
    #Generate today's date folders
    create_folder()
    print('#############################################################################')
    print('-------- Generating customer ALR report ---------')
    
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
    report_types = ["GET_MERCHANT_LISTINGS_ALL_DATA"]
    
    #Get the report object and store it in variable
    res = reports.get_reports(reportTypes=report_types)
    
    report_result=res.payload
    
    #Take documentId from the json data
    report_doc_id = report_result['reports'][0]['reportDocumentId']
    
    #Take createtime from the json data
    created_time = report_result['reports'][0]['createdTime']
    
    created_time = pd.to_datetime(created_time).strftime('%Y_%m_%d_%H_%M')
    
    #ofile_path = r"C:\Users\Siddharth\Amazon_SP_API\latest_report"
    file_name = f'custom_alr_{created_time}.csv'
    
    full_path = f"{raw_file_path}\\{file_name}"
    
    #generate report on prticular path
    reports.get_report_document(f'{report_doc_id}',download=True,file=full_path)
    
    print('------------------------------------------------------')
    print('Custom ALL Listing Report downloaded by SP-API')
    print('------------------------------------------------------')
    print('')
    print("Uploading Raw file on s3")
    print("*********************************************************")
    
    delta_s3_path = f'{s3_path}/delta_files/{run_date}/'
    
    multi_files_upload_s3(raw_file_path,bucket,delta_s3_path)
    
    print("***********************************************************")
    print('Caliing Modified CALR function')
    
    modified_calr(raw_file_path,file_name)

#This function will take raw file as input and create modified file 
def modified_calr(ofile_path,file_name):

    print('#############################################################################')
    print('----------------- Generating Modified Custom ALR ---------------------')
    file_path = ofile_path
    #file_name = file_path.split('\\')[-1]
    file_name = file_name
    
    modified_file_path = mod_file_path
    
    # Try reading the file with different encodings
    try:
        #calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='utf-8', error_bad_lines=False, warn_bad_lines=True)
        calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='utf-8')
    except UnicodeDecodeError:
        try:
            #calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='latin1', error_bad_lines=False, warn_bad_lines=True)
            calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='latin1')
        except UnicodeDecodeError:
            #calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='cp1252', error_bad_lines=False, warn_bad_lines=True)
            calr_df = pd.read_csv(f'{file_path}\\{file_name}',sep='\t', encoding='cp1252')
    
    
    #Required columns to generate report
    calr_columns = ['seller-sku','asin1','item-name','price','quantity','product-id','fulfillment-channel','status','run_date']
    
    #Add run_date columns
    calr_df['run_date'] = datetime.today().strftime('%Y-%m-%d')
    
    #Rearrange columns
    calr_df = calr_df.reindex(columns=calr_columns)
    
    #Generate csv file
    calr_df.to_csv(f'{modified_file_path}\\mod_{file_name}',sep='|',index=False)
    
    print("***********************************************************")
    print("Uploading modified file on s3")
    
    modified_s3_path = f'{s3_path}/modified_files/{run_date}/'
    
    multi_files_upload_s3(modified_file_path,bucket,modified_s3_path)
   
    print('------------------------------------------------------')
    print('Custom ALL Listing Report has been modified')
    print('------------------------------------------------------')
    print('Calling Transform CALR function')
    
    transform_calr(modified_file_path,file_name)
    

#Function will sort the data and remove duplicates ASIN from the file
def transform_calr(file_path,file_name):
    print('#############################################################################')
    print('----------------- Generating Transformed Custom ALR ---------------------')
    
    m_file_path = file_path
    m_file_name = f'mod_{file_name}'
    print('file_name is :',m_file_name)
    
    trans_file_name = m_file_name.replace('mod','transformed')
    print('Transformed file Name is:', trans_file_name)
    
    #trasfomed_file_path = f'C:\\Users\\Siddharth\\Amazon_SP_API\\Transformed_alr'
    trasfomed_file_path = trans_file_path
    
    #Read csv file from the particular folder
    clr_df=pd.read_csv(f'{m_file_path}\\{m_file_name}',sep='|')
    
    #Filter onlu required columns
    clr_df = clr_df[['seller-sku','asin1','product-id','fulfillment-channel','status']]
    
    #Sort columns by fulfillment-channel
    clr_df=clr_df.sort_values(by = ['fulfillment-channel'])
    
    #count how many duplicate and distinct values are there (True:Duplicated data, False : distinct data)
    clr_df.duplicated(subset=['asin1'],keep=False).value_counts()
    
    #Create seperate Datafram for distinct data
    clr_df_distinct = clr_df[clr_df.duplicated(subset=['asin1'],keep=False) == False].sort_values(by=['fulfillment-channel'])
    
    #Create seperate DF for duplicated data and sort it by 'fulfillment-channel' & 'status'  
    clr_df_dups=clr_df[clr_df.duplicated(subset=['asin1'],keep=False) == True].sort_values(by=['fulfillment-channel','status'])
    
    #Remove duplicates data and keep first records
    clr_df_dups = clr_df_dups.drop_duplicates(subset=['asin1'],keep='first')
    
    #append distinct data DF to duplicates data DF
    final_calr_df = clr_df_dups._append(clr_df_distinct)
    
    #Required columns to generate report
    calr_columns = ['seller-sku','asin1','product-id','fulfillment-channel','status','active_flag','run_date']
    
    #Add active_flag column
    final_calr_df['active_flag'] = 0
    
    #Add run_date columns
    final_calr_df['run_date'] = datetime.today().strftime('%Y-%m-%d')
    
    #Rearrange columns
    final_calr_df = final_calr_df.reindex(columns=calr_columns)
    
    final_calr_df.to_csv(f'{trasfomed_file_path}\\{trans_file_name}',sep='|',index=False)
    
    print('------------------------------------------------------')
    print("Uploading Transformed file on s3")
    
    transf_s3_path = f'{s3_path}/transformed_files/{run_date}/' 
    multi_files_upload_s3(trasfomed_file_path,bucket,transf_s3_path) 
    print('******************************************************')
 
    print('Create trigger file on s3')
    
    trigger_path = f'{s3_path}/trigger_files/custom_alr_{run_date}.txt'
    
    create_empty_file_on_s3(bucket,trigger_path)
    #print(f"empty file created on {trigger_path}")
    
    print('******************************************************')
    print('Custom ALL Listing Report has been transformed')
    print(f'Report file path: {trasfomed_file_path}\\{trans_file_name}')
    print('############################################################')
    create_script_status_file("custom_alr_main","weekly(Mon)","Marketplace","cbiankitw10",starttime,datetime.now().strftime('%Y%m%d%H%M%S'))
    
    #Calling function to send completion email
    script_competion_notification()
 
if __name__ == "__main__":
   print('******************************************************')
   print("Function Called : generate custom All Listing Report")
   generate_custom_alr()