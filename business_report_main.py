from datetime import datetime, timedelta
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.api import Reports
import pandas as pd
import json
from s3_uploader import *
import os
import time
import glob
import sys
import subprocess
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *

"""
NAME: business_report_main.py
CREATED DATE: 16-Jul-2024
Created By : Siddharth Thorat
DESCRIPTION: This script download Business Report from Amazon using SP-API ,modified it and 
             upload it on S3.
================================================================================================================
"""

script_path = r"D:\MPI\business_report"
json_f_path = r"D:\MPI\business_report\daily_files\json_files"
mod_f_path = r"D:\MPI\business_report\daily_files\modified_files"
bucket = 'desototech'

s3_path = 'Marketplace/API_Extraction/MPI_reports/business_reports'
#s3_path = 'DWH_team/sid/MPI_Reports/Business_report'
log_dir = r"D:\MPI\business_report\logs"
run_dt = datetime.now().strftime('%Y%m%d')
starttime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#Creat a log file 
sys.stdout = open(f"{log_dir}\\business_report_{run_dt}.log", 'a')

os.chdir(script_path)

#Log file for email notifications
log_file_email = f"{log_dir}\\email_notication_{run_dt}.log"

#Generic email function to send start and completion emails
def call_email_script(subject,body):
    config_file = "email_config.config"
    
    email_start_subject = subject
    email_start_body = body
    
    print("---------------------------------------------------------")
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"{script_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)


def script_start_notification():

    email_start_subject = f"MPI Business Report Daily extraction started for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
Job business_report_main.py for extraction of Business Report Daily has started for {run_dt}

Start time : {starttime}
"""

    #Call email send script function and pass subject and body
    call_email_script(email_start_subject,email_start_body)
    
def script_competion_notification():
    
    email_completion_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    email_start_subject = f"MPI Business Report Daily extraction completed for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
Job business_report_main.py for extraction of Business Report Daily has completed for {run_dt}

Start time : {starttime}
Completion time : {email_completion_time}
"""

    #Call email send script function and pass subject and body
    call_email_script(email_start_subject,email_start_body)

def report_not_found():
    
    email_completion_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    email_start_subject = f"Alert!MPI Business Report Daily not found for {run_dt}"
    #print(email_start_subject)
    email_start_body = f"""
MPI business report Daily is not found for {run_dt}

Start time : {starttime}
Completion time : {email_completion_time}
"""

    #Call email send script function and pass subject and body
    call_email_script(email_start_subject,email_start_body)

#This function will create run_date folder in Json and modified file path folders
def create_folder():
    #Get current date
    today = datetime.now().strftime("%Y%m%d")
    two_days_before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    #two_days_before = '2024-07-06'
    
    print('------------------------------------------------------------------------')
    print("Current date is: ",today)
    print("Two Days before date is: ",two_days_before)
    print('------------------------------------------------------------------------')
    #out_file_path = f'{ofile_path}\\{today}'
    glob_folders = []
    
    folders_list = [json_f_path,mod_f_path]
    
    for folder in folders_list:
        run_date_folder = os.path.join(folder,today)
        if not os.path.exists(run_date_folder):
            os.mkdir(run_date_folder)
            print("Folder is created:",run_date_folder)
            glob_folders.append(run_date_folder)
            
        else:
            print(f'Path is already exists: ',run_date_folder)
            glob_folders.append(run_date_folder)
    
    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_date'] = today
    listOfGlobals['two_days_before'] = two_days_before
    listOfGlobals['json_file_path'] = glob_folders[0]
    listOfGlobals['mod_file_path'] = glob_folders[1]
    
    print('********************************************')
    print('Json file path is:',json_file_path)
    print('********************************************')
    print('Modified file path is:',mod_file_path)
    print('********************************************')   

#This function will read the credential file and get the tokens,id and client secret details
def read_json_file():
    
    # Write the JSON object to a file
    json_path = r'D:\MPI\business_report\credentials.json'
    
    with open(json_path) as json_file:
        json_data = json.load(json_file)
    
    refresh_token = json_data['refresh_token']
    lwa_app_id = json_data['lwa_app_id']
    lwa_client_secret = json_data['lwa_client_secret']
    
    return refresh_token,lwa_app_id,lwa_client_secret


#Fucntion will call SP-API and get the Business Report raw file which is in JSON format
def generate_business_report():
    #Start email notification
    script_start_notification()
    
    #Generate today's date folders
    create_folder()
    
    print('-------- Generating Business report ---------')
    
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
    report_types = ["GET_SALES_AND_TRAFFIC_REPORT"]
    
    #Filter Report based on CHILD level of Granularity
    report_options = {"asinGranularity": "CHILD"}
    
    #Get the report object and store it in variable
    #res = reports.get_reports(reportTypes=report_types)
    
    res = reports.get_reports(reportTypes=report_types,dataStartTime=f'{two_days_before}T00:00:00+00:00',reportOptions=report_options)
    
    report_result=res.payload
    
    report_doc_id = []
    
    for i in range(len(report_result['reports'])-1,-1,-1):
        data_start_time = pd.to_datetime(report_result['reports'][i]['dataStartTime']).strftime('%Y-%m-%d')
        report_docs_id = report_result['reports'][i]['reportDocumentId']
        print(f"Date : {data_start_time} - report_id : {report_docs_id}")

        file_name = f'business_report_{data_start_time}.json'
        print('File Name is : ',file_name)

        full_path = f"{json_file_path}\\{file_name}"
        print('Full File path is :',full_path)
        print('_______________________________________________________________________________________________________')
    
        #generate report on prticular path
        reports.get_report_document(f'{report_docs_id}',download=True,file=full_path)
        time.sleep(5)
        
        #call modified file function
        modified_business_report(json_file_path,file_name,data_start_time)
   
    print('******************************************************')
    print('Business Report downloaded by SP-API')
    print('******************************************************')
    print('')
    print("*********************************************************")
    print("Uploading JSON Business Report file on s3")
    
    delta_s3_path = f'{s3_path}/json_files/{run_date}/'
    multi_files_upload_s3(json_file_path,bucket,delta_s3_path)
    
    print("*********************************************************")
    print("Uploading CSV Business Report file on s3")
    
    mod_s3_path = f'{s3_path}/modified_files/{run_date}/'
    mod_s3_path = f'{s3_path}/modified_files/{run_date}/mod_business_report_{two_days_before}.csv' # enable for only single file
    upload_to_s3(f'{mod_file_path}\\mod_business_report_{two_days_before}.csv',bucket,mod_s3_path) # enable for only single file
    #multi_files_upload_s3(mod_file_path,bucket,mod_s3_path)
    
    print("***********************************************************")
    
    print('JSON and CSV files have been uploaded on s3!!')
    
    
    trigger_path = f'{s3_path}/trigger_files/business_report_{run_date}.txt'
    
    create_empty_file_on_s3(bucket,trigger_path)
    print("***********************************************************")
    print(f"empty file created on {trigger_path}")
    
    print("***********************************************************")
    #Send script completion email
    script_competion_notification()
    print("Script completed : ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    #this function will create script compeletion file 
    create_script_status_file("business_report_main","daily","Marketplace","cbiankitw10",starttime,datetime.now().strftime('%Y%m%d%H%M%S'))

#This function will flatten the JSON data then add few columns and generate the modified CSV file
def modified_business_report(o_file_path,f_name,file_date):
    json_file_path = o_file_path
    file_name = f_name
    mod_file_name = f_name.split('.')[0]
    file_date = file_date
    year = file_date.split('-')[0]
    
    full_json_path = os.path.join(json_file_path,file_name)
    print("JSON File Path is: ",full_json_path)
    
    #Read JSON file
    with open(full_json_path,'r') as file:
        data = json.load(file)
    
    #Ge data only based on SalesAndTrafficByAsin attribute from Json data
    asin_data=data['salesAndTrafficByAsin']
    
    #Normlize Json data into DataFrame
    br_reports=pd.json_normalize(asin_data)
    
    #Add New columns as below
    br_reports['year'] = file_date.split('-')[0]
    br_reports['file_date'] = f'{file_date}'
    br_reports['file_name'] = f'{file_name}'
    br_reports['run_date'] =  datetime.now().strftime("%Y-%m-%d")
    
    #Final columns to be in file
    br_col = ['parentAsin','childAsin','trafficByAsin.mobileAppSessions','trafficByAsin.mobileAppSessionsB2B','trafficByAsin.browserSessions','trafficByAsin.browserSessionsB2B','trafficByAsin.sessions', 'trafficByAsin.sessionsB2B','trafficByAsin.mobileAppSessionPercentage', 'trafficByAsin.mobileAppSessionPercentageB2B','trafficByAsin.browserSessionPercentage','trafficByAsin.browserSessionPercentageB2B','trafficByAsin.sessionPercentage', 'trafficByAsin.sessionPercentageB2B','trafficByAsin.mobileAppPageViews','trafficByAsin.mobileAppPageViewsB2B','trafficByAsin.browserPageViews', 'trafficByAsin.browserPageViewsB2B','trafficByAsin.pageViews','trafficByAsin.pageViewsB2B','trafficByAsin.mobileAppPageViewsPercentage','trafficByAsin.mobileAppPageViewsPercentageB2B','trafficByAsin.browserPageViewsPercentage','trafficByAsin.browserPageViewsPercentageB2B','trafficByAsin.pageViewsPercentage','trafficByAsin.pageViewsPercentageB2B','trafficByAsin.buyBoxPercentage', 'trafficByAsin.buyBoxPercentageB2B','salesByAsin.unitsOrdered','salesByAsin.unitsOrderedB2B', 'trafficByAsin.unitSessionPercentage', 'trafficByAsin.unitSessionPercentageB2B','salesByAsin.orderedProductSales.amount','salesByAsin.orderedProductSalesB2B.amount','salesByAsin.totalOrderItems', 'salesByAsin.totalOrderItemsB2B','year','file_date','file_name','run_date']
    
    #Arrange columns
    br_reports=br_reports.reindex(columns=br_col)
    
    
    percentage_columns = [
    'trafficByAsin.mobileAppSessionPercentage',
    'trafficByAsin.mobileAppSessionPercentageB2B',
    'trafficByAsin.browserSessionPercentage',
    'trafficByAsin.sessionPercentage',
    'trafficByAsin.sessionPercentageB2B',
    'trafficByAsin.mobileAppPageViewsPercentage',
    'trafficByAsin.mobileAppPageViewsPercentageB2B',
    'trafficByAsin.browserPageViewsPercentage',
    'trafficByAsin.browserPageViewsPercentageB2B',
    'trafficByAsin.pageViewsPercentage',
    'trafficByAsin.pageViewsPercentageB2B',
    'trafficByAsin.buyBoxPercentage',
    'trafficByAsin.buyBoxPercentageB2B',
    'trafficByAsin.unitSessionPercentage',
    'trafficByAsin.unitSessionPercentageB2B'
    ]
    
    # Add percentage symbol to the specified columns
    for col in percentage_columns:
        br_reports[col] = br_reports[col].astype(str) + '%'
    
    #Generate CSV file in modified file folder
    br_reports.to_csv(f'{mod_file_path}\\mod_{mod_file_name}.csv',index=False)
    
    print("-------------------------------------------------------------------------")
    print(f"Modified(CSV) file path : {mod_file_path}\\mod_{mod_file_name}.csv")
    print("-------------------------------------------------------------------------")
    
if __name__ == "__main__":
   print('******************************************************')
   print("Function Called : generate Business Report - Amazon SP-API")
   generate_business_report()