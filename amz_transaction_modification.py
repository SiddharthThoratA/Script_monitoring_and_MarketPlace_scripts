import sys, getopt
import datetime
import boto3
import csv
import gzip
import os
import psycopg2
import pandas as pd
import logging
from pytz import timezone, UnknownTimeZoneError
from googletrans import Translator
import time
from datetime import datetime, timedelta

#run_date = datetime.datetime.now().strftime("%Y%m%d")
run_date = ''

"""
how to call this script :
python3 amz_transaction_modification.py -c <config_file> -d <run_dt>
example :
python3 amz_transaction_modification.py -c amz_transaction_redshift.config -d 20240325
"""

def main(argv):

    try:
        opts, args = getopt.getopt(argv, "hd:c:t:", ["date=", "config=", "type="])
    except getopt.GetoptError:
        print('amz_transaction_modification.py -c <config_file> -d <rundate>')
    for opt, arg in opts:
        if opt == '-h':
            print('amz_transaction_modification.py -c <config_file> -d <rundate>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
            print('rundate here :',run_dt)
        elif opt in ('-c', '--config'):
            config_file = arg
        elif opt in ('-t','--type'):
            file_type = arg
            if not config_file:
                print('Missing config file name --> syntax is : amz_transaction_modification.py -c <config_file> -d <rundate>')
                sys.exit()

    print('config file is :', config_file)
    print('File type is: ',file_type)
    
    listOfGlobals = globals()
    listOfGlobals['run_dt'] = run_dt
    listOfGlobals['file_type'] = file_type
    
    print("************************************")
    print('Previous date is :',run_dt)
    print("************************************")
    
    logging.basicConfig(filename=f'D:\MPI\Transactions\log_files\column_mismatched_file_{run_dt}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    formatted_date = datetime.strptime(run_dt, "%Y%m%d").strftime("%Y-%m-%d")
    
    print('Formated date is: ',formatted_date)
    print("************************************")
    
    listOfGlobals['formatted_date'] = formatted_date
    
    read_config(config_file)
    
def read_config(config_file):
    conf_file = config_file
    fileobj = open(conf_file)
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('=')
            print('conf_value before: ',conf_value)
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
            print('conf_value after: ',conf_value)
    fileobj.close()

    print(params)
    
    listOfGlobals = globals()
    listOfGlobals['params'] = params
    
    
    #call this function to use values from the created dictionary and create directory in Unix
    ex_param_file(params)

    print('state : complete')


def ex_param_file(params):
    #print('inside ex_sql_file()')
        
    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC'].strip()
        ofilepath = ofilepath.replace('RUN_DATE',run_dt)
        print("ofilepath is - ",ofilepath)
        
        #create a date folder in given path
        try:
            os.mkdir(ofilepath)
        except OSError:
            print ("Creation of the directory %s failed" % ofilepath)
        else:
            print ("Successfully created the directory %s " % ofilepath)
        
         #Create a below mentioned folders inside the created date folder
        folder_names = ['CA','Mexico','USA']
        mod_file_paths = []
        for i in range(len(folder_names)):
            folder_path = os.path.join(ofilepath,folder_names[i])
            mod_file_paths.append(folder_path)
     
            try:
                os.mkdir(folder_path)
            except OSError:
                print ("Creation of the directory %s failed" % folder_path)
            else:
                print ("Successfully created the directory %s " % folder_path)
        
    else:
        print('Missing output location path --- exiting')
        sys.exit()
        
    print("File paths: ", mod_file_paths)
     
    #Calling read_list_file function
    check_file_type(mod_file_paths)

#check file_type wheter it is CA,USA or Mexico
def check_file_type(mod_file_paths):
    ca_mod_file_path = mod_file_paths[0]
   
    header_ca = ['date/time', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'gift wrap credits tax', 'Regulatory fee', 'Tax on regulatory fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other','total']
    
    numeric_columns = [ 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'gift wrap credits tax', 'Regulatory fee', 'Tax on regulatory fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other', 'total']
    
    num_col =['order postal', 'settlement id']
    
    #Final cols
    final_cols = ['data_time_orignal', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'gift wrap credits tax', 'Regulatory fee', 'Tax on regulatory fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other','total','file_name', 'run_date', 'date_time_pst']
    
    
    
    if file_type == 'CA':
        country = 'CA'
        if params['srs_file_path']:
            file_path = params['srs_file_path'].strip()
            ca_srs_file_path = os.path.join(file_path, "CA", run_dt, "combined_file\\")
            print('CA file_path is: ', ca_srs_file_path)
            
            # Check if the directory exists
            if os.path.exists(ca_srs_file_path):
            
                # List all files in the directory
                for file_name in os.listdir(ca_srs_file_path):
                    ca_full_file_path = os.path.join(ca_srs_file_path, file_name)
            
                    # Process each file 
                    if os.path.isfile(ca_full_file_path):  # Ensure it's a file
                        print(f'Processing file: {ca_full_file_path}')
                        amz_tran_file_modification(ca_full_file_path,ca_mod_file_path,numeric_columns,final_cols,country,header_ca,num_col)  
        
            else:
                print(f"The directory {ca_srs_file_path} does not exist.")
        
    elif file_type == 'Mexico':
        mx_mod_file_path = mod_file_paths[1]
        country = 'MX'
        
        if params['srs_file_path']:
            file_path = params['srs_file_path'].strip()
            mx_srs_file_path = os.path.join(file_path, "Mexico", run_dt, "combined_file\\")
            print('Mexico file_path is: ', mx_srs_file_path)
            
            # Check if the directory exists
            if os.path.exists(mx_srs_file_path):
                # List all files in the directory
                for file_name in os.listdir(mx_srs_file_path):
                    mx_full_file_path = os.path.join(mx_srs_file_path, file_name)
            
                    # Process each file (you can replace this with your actual processing logic)
                    if os.path.isfile(mx_full_file_path):  # Ensure it's a file
                        print(f'Processing file: {mx_full_file_path}')
                        mexico_modification_csv(mx_full_file_path,mx_mod_file_path)
            else:
                print(f"The directory {mx_srs_file_path} does not exist.")
        
    elif file_type == 'USA':
        us_mod_file_path = mod_file_paths[2]
        
        country = 'US'
        header_us = [ 'date/time', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'account type', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other', 'total' ]
        
        numeric_columns = [ 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other', 'total']
        
        num_col =['order postal', 'settlement id']
        
        #Final columns
        final_cols = ['data_time_orignal', 'settlement id', 'type', 'order id', 'sku', 'description', 'quantity', 'marketplace', 'account type', 'fulfillment', 'order city', 'order state', 'order postal', 'tax collection model', 'product sales', 'product sales tax', 'shipping credits', 'shipping credits tax', 'gift wrap credits', 'giftwrap credits tax', 'Regulatory Fee', 'Tax On Regulatory Fee', 'promotional rebates', 'promotional rebates tax', 'marketplace withheld tax', 'selling fees', 'fba fees', 'other transaction fees', 'other', 'total', 'file_name', 'run_date', 'date_time_pst']
        
        if params['srs_file_path']:
            file_path = params['srs_file_path'].strip()
            us_srs_file_path = os.path.join(file_path, "USA", run_dt, "combined_file\\")
        
            print('USA file_path is: ', us_srs_file_path)
        
            # Check if the directory exists
            if os.path.exists(us_srs_file_path):
                # List all files in the directory
                for file_name in os.listdir(us_srs_file_path):
                    US_full_file_path = os.path.join(us_srs_file_path, file_name)
        
                    if os.path.isfile(US_full_file_path):  
                        print(f'Processing file: {US_full_file_path}')
                        amz_tran_file_modification(US_full_file_path,us_mod_file_path,numeric_columns,final_cols,country,header_us,num_col)
        
            else:
                print(f"The directory {us_srs_file_path} does not exist.")    
                    
def convert_timezone(date):
    try:
        original_timezone = timezone(str(date.tzinfo))
        target_timezone = timezone('US/Pacific') 
        return date.replace(tzinfo=original_timezone).astimezone(target_timezone)
    except UnknownTimeZoneError:
        return date 

#it is generic function to modified CA and USA file
def amz_tran_file_modification(srs_file_path,mod_file_path,numeric_columns,final_cols,country,header,num_col):
    print(f"{file_type} modification script started")
    
    source_path = srs_file_path
    modification_path = mod_file_path
    country = country
    
    amz_trans = pd.read_csv(f'{source_path}',sep=',',skiprows = 8, header = None, names = header, low_memory=False)
    
    amz_trans['file_name'] = f'Amazon_{country}_Transaction_{formatted_date}.csv'
    amz_trans['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    amz_trans['date_time_pst'] = ''
    
    for col in num_col:
        amz_trans[col] = pd.to_numeric(amz_trans[col], errors='coerce').fillna(0).astype('int64')
    
     
    #Remove ',' from the numeric values and replace with blank
    for i in numeric_columns:
        amz_trans[i] = amz_trans[i].astype(str).str.replace(',','').str.strip().fillna(0).astype(float)
       
    new_amz_trans = amz_trans.copy()
    
    spanish_month_mapping = {
        'ene': 'January',
        'feb': 'February',
        'marzo': 'March',
        'abr': 'April',
        'mayo': 'May',
        'jun': 'June',
        'jul': 'July',
        'ago': 'August',
        'sept': 'September',
        'oct': 'October',
        'nov': 'November',
        'dic': 'December',
        }
        
     
    for spanish_month, english_month in spanish_month_mapping.items():
        amz_trans['date/time'] = amz_trans['date/time'].str.replace(spanish_month, english_month, case=False)
        
        
    amz_trans['date/time'] = amz_trans['date/time'].astype(str).str.replace('\u200B', '').str.strip()
    amz_trans['date/time'] = pd.to_datetime(amz_trans['date/time'], errors='coerce')
    
    final_cols = final_cols

    
    amz_trans['data_time_orignal'] = new_amz_trans['date/time']
    amz_trans['date_time_pst'] = amz_trans['date/time'].apply(convert_timezone)
    
    amz_trans = amz_trans.drop(columns=['date/time'],axis=1)
    amz_trans = amz_trans.reindex(columns=final_cols)
    
    base_file_name = f"{modification_path}/mod_Amazon_{country}_Transaction_{formatted_date}.csv"
    amz_trans.to_csv(base_file_name, index=False)
    print(f"File saved as: {base_file_name}")

def translate_to_english(text):
    try:
        if isinstance(text, str):
            translator = Translator(service_urls=['translate.googleapis.com'])
            translator = Translator()
            translation = translator.translate(text, dest='en')
            return translation.text
        elif isinstance(text, datetime): 
            return text
        else:
            return str(text) 
    except Exception as e:
        print(f"Translation error: {e}")
        return str(text)

    
#This function will modify Mexico file
def mexico_modification_csv(mx_src_file_path,mx_dest_file_path):
    print("Maxico modification function is called")
    header_mx = ['fecha/hora', 'Id. de liquidación', 'tipo', 'Id. del pedido', 'sku', 'descripción', 'cantidad', 'marketplace', 'cumplimiento', 'ciudad del pedido', 'estado del pedido', 'código postal del pedido', 'modelo de recaudación de impuestos', 'ventas de productos', 'impuesto de ventas de productos', 'créditos de envío', 'impuesto de abono de envío', 'créditos por envoltorio de regalo', 'impuesto de créditos de envoltura', 'Tarifa reglamentaria', 'Impuesto sobre tarifa reglamentaria', 'descuentos promocionales', 'impuesto de reembolsos promocionales', 'impuesto de retenciones en la plataforma', 'tarifas de venta', 'tarifas fba', 'tarifas de otra transacción', 'otro', 'total']
    
    source_path = mx_src_file_path
    mx_trans = pd.read_csv(f'{mx_src_file_path}',sep=',',skiprows = 8, header = None, names = header_mx, low_memory=False)
    
    
    mx_trans['file_name'] = f'Amazon_MX_Transaction_{formatted_date}.csv'
    mx_trans['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mx_trans['date_time_pst'] = ''
    
    num_col =['código postal del pedido', 'Id. de liquidación']
    
    numeric_columns = ['ventas de productos' , 'impuesto de ventas de productos' , 'créditos de envío' , 'impuesto de abono de envío' , 'créditos por envoltorio de regalo' , 'impuesto de créditos de envoltura' , 'Tarifa reglamentaria' , 'Impuesto sobre tarifa reglamentaria' , 'descuentos promocionales' , 'impuesto de reembolsos promocionales' , 'impuesto de retenciones en la plataforma' , 'tarifas de venta' , 'tarifas fba' , 'tarifas de otra transacción' , 'otro' , 'total']
    
    for col in num_col:
        mx_trans[col] = pd.to_numeric(mx_trans[col], errors='coerce').fillna(0).astype('int64') 
        
    #Remove ',' from the numeric values and replace with blank
    for i in numeric_columns:
        mx_trans[i] = mx_trans[i].astype(str).str.replace(',','').str.strip().astype(float)
    
    new_mx_trans = mx_trans.copy()
    
    spanish_month_mapping = {
        'ene': 'January',
        'feb': 'February',
        'marzo': 'March',
        'abr': 'April',
        'mayo': 'May',
        'jun': 'June',
        'jul': 'July',
        'ago': 'August',
        'sept': 'September',
        'oct': 'October',
        'nov': 'November',
        'dic': 'December',
        }
        
     
    for spanish_month, english_month in spanish_month_mapping.items():
        mx_trans['fecha/hora'] = mx_trans['fecha/hora'].str.replace(spanish_month, english_month, case=False)
        
        
    mx_trans['fecha/hora'] = mx_trans['fecha/hora'].astype(str).str.replace('\u200B', '').str.strip()
    mx_trans['fecha/hora'] = pd.to_datetime(mx_trans['fecha/hora'], errors='coerce')
    
    final_cols = ['data_time_orignal', 'Id. de liquidación' , 'tipo ' , 'Id. del pedido' , 'sku' , 'descripción' , 'cantidad' , 'marketplace' , 'cumplimiento' , 'ciudad del pedido' , 'estado del pedido' , 'código postal del pedido' , 'modelo de recaudación de impuestos' , 'ventas de productos' , 'impuesto de ventas de productos' , 'créditos de envío' , 'impuesto de abono de envío' , 'créditos por envoltorio de regalo' , 'impuesto de créditos de envoltura' , 'Tarifa reglamentaria' , 'Impuesto sobre tarifa reglamentaria' , 'descuentos promocionales' , 'impuesto de reembolsos promocionales' , 'impuesto de retenciones en la plataforma' , 'tarifas de venta' , 'tarifas fba' , 'tarifas de otra transacción' , 'otro' , 'total',  'file_name', 'run_date', 'date_time_pst']

    
    mx_trans['data_time_orignal'] = new_mx_trans['fecha/hora']
    mx_trans['date_time_pst'] = mx_trans['fecha/hora'].apply(convert_timezone)
    
    mx_trans = mx_trans.drop(columns=['fecha/hora'],axis=1)
    mx_trans = mx_trans.reindex(columns=final_cols)
    
    base_file_name = f"{mx_dest_file_path}\\mod_Amazon_MX_Transaction_{formatted_date}.csv"
    mx_trans.to_csv(base_file_name, index=False)
    print(f"File saved as: {base_file_name}")
    
if __name__ == "__main__":
    main(sys.argv[1:])