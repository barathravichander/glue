# This Script is used to convert csv files into an excel
#----------------------------------------------------------------------------------------------------------------
#                       History
# Date         Version No      Change                                                                 Developer
#-------       ------          ------                                                                 ---------
#03/24/2022     0.1            UPRS-43785: The columns width in each sheet of the report               Utkarsh
#                              should be expanded by default to accommodate the column headers.          
##################################################################################################################

import boto3
import pandas as pd
import io
import sys
import logging
from openpyxl import load_workbook
import math
import xlsxwriter

# Setting up log
logging.basicConfig(format='%(asctime)s %(message)s',filename='/pr_ubsfi/logs/glue/util_excel_merge.log',datefmt='%m/%d/%Y %I:%M:%S %p',filemode='w',level=logging.INFO)

# Setting up boto3
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Take bucket name from arguments
bucket_name = sys.argv[1]
print('Source bucket name :  {}'.format(bucket_name))
logging.info('Source bucket name :  {}'.format(bucket_name))

# Take csv_lookup_loc from arguments
p_csv_lkp_location = sys.argv[2]    
print('csv lookup location :  {}'.format(p_csv_lkp_location))
logging.info('csv lookup location :  {}'.format(p_csv_lkp_location))

# Take destination_location_key from arguments
destination_location_key = sys.argv[3]
print('destination location :  {}'.format(destination_location_key))
logging.info('destination location :  {}'.format(destination_location_key))

# Take input file list from arguments
file_list = sys.argv[4]
print('CSV File List :  {}'.format(file_list))
logging.info('CSV File List :  {}'.format(file_list))

# Take output filename from arguments
excel_filename = sys.argv[5]
print('Output Excel FileName :  {}'.format(excel_filename))
logging.info('Output Excel FileName :  {}'.format(excel_filename))

#Assigning bucket name
bucket = s3.Bucket(bucket_name)
prefix_objs = bucket.objects.filter(Prefix=p_csv_lkp_location)

#Setting csv lookup location
p_csv_lkp_location = 's3://' + bucket_name + '/' + p_csv_lkp_location

# Check if lookup and destination ends with '/'
if p_csv_lkp_location.endswith('/') == False:
    p_csv_lkp_location = p_csv_lkp_location + "/"

if destination_location_key.endswith('/') == False:
    destination_location_key = destination_location_key + "/"

# prefix of the source file
input_prefix = p_csv_lkp_location.split("://",10)
input_prefix = input_prefix[1].split('/',1)
input_prefix = input_prefix[1]

# Writing to Excel
datatoexcel = pd.ExcelWriter(excel_filename)

# Reading objects in lookup location
for obj in prefix_objs:
    key = obj.key

    # Get Filename from incoming_location
    split_key_location = key.split("/", 10)
    file_name = split_key_location[-1]

    file_name_split = file_name.split("_", 10)
    sheetname = file_name_split[0]

    # Checking if file is present in File List
    if file_name in file_list:
        body = obj.get()['Body'].read()
        # Check if body is null
        if body == b'':
            continue
        csv_df = pd.read_csv(io.BytesIO(body), encoding='utf8')

        # Writing csv df to excel   
        csv_df.to_excel(datatoexcel, index=False, sheet_name=sheetname)
        
        #Adjust column width
        for column in csv_df:
            column_width = max(csv_df[column].astype(str).map(len).max(), len(column))
            col_idx = csv_df.columns.get_loc(column)
            
            if math.isnan(column_width):
                column_width = len(column) + 2
                
            if column_width<20:
               column_width = column_width*2
            
            datatoexcel.sheets[sheetname].set_column(col_idx, col_idx, column_width)

# Saving excel file to ec2
datatoexcel.save()

#Load excel workbook
wb = load_workbook(datatoexcel)

#Set the sheet with name ReportCriteria as first sheet
ReportCriteria_sheet_position = wb.worksheets.index(wb['ReportCriteria'])
ReportCriteria_sheet_new_position = 0
sheets = wb._sheets.copy()
sheets.insert(ReportCriteria_sheet_new_position, sheets.pop(ReportCriteria_sheet_position))
wb._sheets = sheets
wb.save(excel_filename)

# Concatenating destination key and excel filename
destination_location = destination_location_key + excel_filename

# Reading and writing excel file to destination s3 location
with open(excel_filename,'rb') as f:
    try:
        s3_client.upload_fileobj(f, bucket_name, destination_location)
        print('File: {} Uploaded Successfully'.format(excel_filename))
        logging.info('File: {} Uploaded Successfully'.format(excel_filename))
    except Exception as e:
        print('File Upload Failed...')
        print('Error {} '.format(e))
        logging.info('File Upload Failed...')
        logging.info('Error {} '.format(e))
        
