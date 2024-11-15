import pandas as pd
from datetime import datetime, timedelta
import sys, getopt
import os
import glob
import requests
from requests.auth import HTTPBasicAuth
import json

# Get the current date in the format YYYYMMDD for folder creation and file management
run_date = datetime.now().strftime('%Y%m%d')
run_dt= datetime.now().strftime('%Y%m%d%H%M')
script_path = f"D:\\script_monitoring"
log_dir = f"{script_path}\\log"
#run_date = '20240912'

sys.stdout = open(f"{log_dir}\\script_monitoring_{run_dt}.log", 'w')
sys.stderr = open(f"{log_dir}\\script_monitoring_{run_dt}.log", 'a')

print(f"***********************************************")
print(f"run date is : {run_date}")
print(f"script path is : {script_path}")
print(f"script start time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('\n')

# Get the day of the week as a numeric value (Monday=0, Sunday=6)
current_date = datetime.now()
day_of_week_numeric = current_date.weekday()

# Define file paths based on day of the week and run date
list_file_path = f"D:\\script_monitoring\\list_files\\list_files_{day_of_week_numeric}.csv"
print(f"Today's list file with path: {list_file_path}\n")

# Load the CSV containing the list of all scripts to be monitored
all_scripts_df = pd.read_csv(list_file_path)

# Define paths for daily scripts, combined script files, and pending scripts
daily_script_path = f"D:\\script_monitoring\\daily_scripts"
daily_script_combined_path = f"D:\\script_monitoring\\daily_scripts_combined"
pending_script_path = f"D:\\script_monitoring\\pending_scripts"
pending_script_combined_path = f"D:\\script_monitoring\\pending_scripts_combined"

# Function to create a folder for the current run date in the specified paths
def create_run_date_folder():
    script_path=[daily_script_path,daily_script_combined_path,pending_script_path,pending_script_combined_path]
    for path in script_path:
        run_date_folder = os.path.join(path,run_date)
        if not os.path.exists(run_date_folder):
            os.mkdir(run_date_folder)
            print("Folder is created:",run_date_folder)
        else:
            #print(f'Path is already exists: ',run_date_folder)
            pass

# Function to combine individual script status CSV files into one
def combined_script_monitor_files():
    
    print(f"\nCombine individual script status CSV files into one has started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Define the required fields/columns for the combined file
    fields=['scriptname','freq','process','server','starttime','endtime','total_completion_time']
    
    # Get all CSV files from the current run date folder
    script_csv_files = glob.glob(f'{daily_script_path}\\{run_date}\\*.csv')
    combined_scripts_csv = pd.DataFrame()
    
    # Append all individual script CSV files into one DataFrame
    for file in script_csv_files:
        df = pd.read_csv(file,sep=',')
        combined_scripts_csv = combined_scripts_csv._append(df, ignore_index=True)
    
    # If no CSVs are found, create an empty DataFrame with predefined headers
    if combined_scripts_csv.empty:
        combined_scripts_csv = pd.DataFrame(columns=fields)
        combined_scripts_csv.to_csv(f"{daily_script_combined_path}\\{run_date}\\combined_file.csv",index=False)
    else:
        combined_scripts_csv.to_csv(f"{daily_script_combined_path}\\{run_date}\\combined_file.csv",sep=',',index=False)
    
    print(f"Combine individual script status CSV files into one has completed at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return combined_scripts_csv

# Function to identify pending scripts with daily frequency that haven't run yet
def pending_script_daily_once():
    
    print(f"\nPending script daily once freqency has started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # Filter scripts scheduled to run before the current time and have a daily frequency
    #daily_once_script_df = all_scripts_df[(all_scripts_df['schedule_time'] < datetime.now().strftime('%H:%M:%S')) & (all_scripts_df['frequency']=='daily')]
    #daily_once_script_df = all_scripts_df[(all_scripts_df['ETA'] < datetime.now().strftime('%H:%M:%S')) & (all_scripts_df['frequency']=='daily')]
    daily_once_script_df = all_scripts_df[
    (all_scripts_df['ETA'] < datetime.now().strftime('%H:%M:%S')) &
    (all_scripts_df['frequency'].str.contains('daily|weekly', case=False, na=False))
]
    
    # Merge with the combined script status to check which ones are pending
    merge_df_1 = pd.merge(daily_once_script_df, combined_df, on='scriptname', how='left')
    pending_script_df_1 = merge_df_1.loc[merge_df_1['freq'].isnull(), ['scriptname','schedule_time', 'frequency', 'scriptpath']]
    
    # Print and save pending scripts to CSV
    #print(pending_script_df_1)
    pending_script_df_1.to_csv(f"{pending_script_path}\\{run_date}\\pending_scripts_daily_once_{run_date}.csv",index=False)
    print(f"Pending script daily once freqency has completed at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# Function to identify pending scripts with multiple frequencies
def pending_script_daily_more_than_one_freq(freqency,hr):
    
    print(f"\nPending daily {freqency} script has started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # Filter scripts based on frequency and missing schedule time
    daily_multiple_script_df = all_scripts_df[all_scripts_df['frequency']==freqency]
    
    # To avoid run time warning, creating one copy of dataframe
    daily_multiple_script_df = daily_multiple_script_df.copy()
    
    # Fill missing schedule times with the current timestamp
    daily_multiple_script_df.loc[:, 'schedule_time_updated'] = daily_multiple_script_df['schedule_time'].fillna(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Merge with the combined script status
    merge_df_2 = pd.merge(daily_multiple_script_df, combined_df, on='scriptname', how='left')
    
    # Convert time columns to datetime format and calculate time difference
    merge_df_2['schedule_time_updated'] = pd.to_datetime(merge_df_2['schedule_time_updated'], format='%Y-%m-%d %H:%M:%S')
    merge_df_2['endtime'] = pd.to_datetime(merge_df_2['endtime'], format='%Y-%m-%d %H:%M:%S')
    merge_df_2['time_difference'] = merge_df_2['schedule_time_updated'] - merge_df_2['endtime']
    merge_df_2['time_difference'] = pd.to_timedelta(merge_df_2['time_difference'])
    
    # Filter scripts based on time difference exceeding a threshold or being null
    threshold = timedelta(hours=hr)
    merge_df_2_final = merge_df_2[(merge_df_2['time_difference'] > threshold) | (merge_df_2['time_difference'].isnull())]
    
    # Save the pending scripts to CSV
    pending_script_df_2 = merge_df_2_final[['scriptname', 'schedule_time','frequency', 'scriptpath']]
    #print(pending_script_df_2)

    pending_script_df_2.to_csv(f"{pending_script_path}\\{run_date}\\pending_scripts_{freqency}_{run_date}.csv",index=False)
    print(f"Pending daily {freqency} script has completed at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# Function to combine pending scripts into a single file
def pending_script_combined_file():
    
    print(f"\nCombine pending scripts into a single file has started at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # Get all pending script CSVs from the current run date folder
    pending_script_csv_files = glob.glob(f'{pending_script_path}\\{run_date}\\*.csv')
    pending_combined_scripts_csv = pd.DataFrame()
    
    # Append all pending scripts CSVs into one DataFrame
    for file in pending_script_csv_files:
        df = pd.read_csv(file,sep=',')
        pending_combined_scripts_csv = pending_combined_scripts_csv._append(df, ignore_index=True)
    
    # Save the combined pending scripts to a CSV file
    pending_combined_scripts_csv.to_csv(f"{pending_script_combined_path}\\{run_date}\\pending_script_combined.csv",sep=',',index=False)
    print(f"Combine pending scripts into a single file has completed at: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'\n')
    return pending_combined_scripts_csv

# Function to send notifications to Microsoft Teams
def send_message_to_teams(message):
    
    # Example usage of Teams webhook URL for sending notifications
    #Testing URL
    #webhook_url = "https://o365spi.webhook.office.com/webhookb2/4ec10c2b-8402-4ea2-9af6-4280be5bee2f@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/b743bb62556940888dd442217de8d0ef/329a3b08-1316-452c-a496-6ab47e68127d"  
    
    #Original URL
    webhook_url = "https://o365spi.webhook.office.com/webhookb2/cf58324e-90f5-474f-a5d1-c98e7a5521dc@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/c90ec851157c427396ef4b8a43ea0773/ec0a6802-264d-45c9-b33d-cd6e6e468db8/V2HHkJLdFU1dVm-mQLSeFrr9TlqXHJRnV5JsLmsUVW5z41"
    
    headers = {'Content-Type': 'application/json'}

    payload = {
        "text": message
    }
    
    # Send the message via POST request and handle response
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print("\nMessage sent successfully to Microsoft Teams!")
    else:
        print(f"Failed to send message to Microsoft Teams. Status code: {response.status_code}")

##################Calling different function#########################

# Create necessary folders for the current run date
create_run_date_folder()

# Combine all script status files into one
combined_df = combined_script_monitor_files()

# Check for pending scripts that are daily and should have already run
pending_script_daily_once()

# Check for pending scripts that run multiple times a day

# Get the current time in HH:MM format
current_time = datetime.now().strftime('%H:%M')
print("\nCurrent time (HH:MI):", current_time)

# Define the comparison time (02:05)
comparison_time = '02:05'

# Check if the current time is greater than 02:05
if current_time > comparison_time:
    pending_script_daily_more_than_one_freq('6hour', 7)
    pending_script_daily_more_than_one_freq('3hour', 3)
    pending_script_daily_more_than_one_freq('2hour', 2)
    pending_script_daily_more_than_one_freq('hourly', 2)
    pending_script_daily_more_than_one_freq('30min', 2)
    #pending_script_daily_more_than_one_freq('15min', 2)
    pending_script_daily_more_than_one_freq('10min', 2)
else:
    print("Current time is not greater than 02:05, skipping function calls for pending scripts that run multiple times a day.")

# Combine all pending scripts into one file
pending_combined_scripts_df = pending_script_combined_file()

# Sending pending scripts information to Microsoft Teams
pending_scripts_info = pending_combined_scripts_df[['scriptname', 'schedule_time','frequency', 'scriptpath']]

# Convert the information into a formatted string for Teams message
scripts_info_string = '  \n'.join(
    #[f"{row['scriptname']}\t{row['frequency']}\t{row['scriptpath']}" 
    [f"{row['scriptname']}---{row['schedule_time']}---{row['frequency']}---{row['scriptpath']}"
     for index, row in pending_scripts_info.iterrows()]
)

# Check if any pending scripts and send the appropriate message to Teams
if pending_combined_scripts_df.empty:
    time_list = ['07:15','23:15']
    
    if current_time in time_list:
        print("no pending script as of now.")
        send_message_to_teams("No pending script as of now.")  
    else:
        print("no pending script as of now.")
else:
    message = f"Pending script list :\n\nscriptname---schedule_time---frequency---scriptpath\n\n{scripts_info_string}"
    print(message)
    send_message_to_teams(message)

print(f"\nscript completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))