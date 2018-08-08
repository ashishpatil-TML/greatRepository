
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import datetime
import pathlib
import teradata
import cx_Oracle
from numpy import nan

start = datetime.datetime.now() # Code start time
## Git validation
## Get path where scripts is stored.
## Required folders and files will be created based on this path.
os.chdir('C:\\Ashish\\codes\\project')
path = os.getcwd()



# In[69]:


#Make a TERADATA Database connection
udaExec = teradata.UdaExec (appName="CoonectTeradata", version="1.0",logConsole=False)
td_con = udaExec.connect(method="odbc", dsn="TD_PROD_AD_USER", transactionMode="Teradata");

#Make a ORACLE Database connection
ora_con = cx_Oracle.connect('usr_migration/usr_migration@172.18.77.124/xe')


# In[4]:


## Function to Retrieve columns for a given Table
## 'DATE_COLUMNS' refers to column having DATE and TIMESTAMP columns casted to Character Date formats
def get_fields (tablename):
    td_columns = ''
    ora_columns = ''
    column_names = []
    for idx,row in master[master['TABLE']==tablename].iterrows():
        td_columns = td_columns +  row['TD_COLS']
        ora_columns = ora_columns +  row['ORA_COLS']
        column_names.append(row['COLUMN'])
    return td_columns,ora_columns,column_names

## Function to get WHERE conditions from master Excel to prepare Teradata and Oracle queries
def get_clauses (tablename):
    td_where_clause = ''
    ora_where_clause = ''
    for idx,row in master[master['TABLE']==tablename].iterrows():
        td_where_clause  = td_where_clause + row['SRC_CONDITION']
        ora_where_clause = ora_where_clause + row['TGT_CONDITION']
        if td_where_clause != '' and ora_where_clause != '':
            break;
    return td_where_clause,ora_where_clause

## Function to compare two dataframes
def compare_two_dfs(input_df_1, input_df_2):
    df_1, df_2 = input_df_1.copy(), input_df_2.copy()
    ne_stacked = (df_1 != df_2).stack()
    changed = ne_stacked[ne_stacked]
    changed.index.names = ['INDEX_VALUES', 'COLUMNS']
    difference_locations = np.where(df_1 != df_2)
    changed_from = df_1.values[difference_locations]
    changed_to = df_2.values[difference_locations]
    df = pd.DataFrame({'TD_VALUE': changed_from, 'ORA_VALUE': changed_to}, index=changed.index)
    df.dropna(how='all', inplace=True)
    return df



## Get Table details from master Excel
master = pd.read_excel (path +'\\Table_column_datatype_V2.0.xlsx' )

## Prepare unique list of tables from Master Excel for creating dataframes
table_list = master.iloc[:,1].unique()
#table_list = ['W_ASSESS_D']


## Creates directory if it doesn't exists and Doesn't throw any Exception even if the directory exists
pathlib.Path(path + '\data_mismatch_files').mkdir(parents=True, exist_ok=True) 

logfile = open('log_table_validations.txt', 'a')
logfile.write('TABLE_NAME | VALIDATION_STATUS | SRC_COUNT | TGT_COUNT| COMMENTS' +'\n')


for tablename in table_list:
    td_cols,ora_cols,column_names = get_fields(tablename)
    td_clause,ora_clause = get_clauses(tablename)
    td_query  = 'SELECT ' +  td_cols + ' FROM ' + tablename + ' WHERE ' + td_clause +';'
    ora_query = 'SELECT ' +  ora_cols + ' FROM ' + tablename + ' WHERE ' + ora_clause 
    
    td_df = pd.read_sql(td_query,td_con)
    td_df.columns=column_names
    td_count = len(td_df)
    
    ## -------- Code to Check if Tables has ROW_WID or INTEGRATION_D ---------##
    if 'ROW_WID' in td_df.columns.str.upper():
        df1 = td_df.set_index('ROW_WID')
        df1.sort_index(axis=0,inplace=True)
        # Setting Oracle dataframe
        ora_df = pd.read_sql(ora_query,ora_con)
        ora_df.columns=column_names
        df2 = ora_df.set_index('ROW_WID')
        df2.sort_index(axis=0,inplace=True)
        ora_count = len(df2)
                             
    elif 'INTEGRATION_ID' in td_df.columns.str.upper():
        df1 = td_df.set_index('INTEGRATION_ID')
        df1.sort_index(axis=0,inplace=True)
        # Setting Oracle dataframe
        ora_df = pd.read_sql(ora_query,ora_con)
        ora_df.columns=column_names
        df2 = ora_df.set_index('INTEGRATION_ID')
        df2.sort_index(axis=0,inplace=True)
        ora_count = len(df2)
    else:
        #ora_df = pd.read_sql(ora_query,ora_con)
        #ora_count = len(ora_df)
        log = tablename +'|'+'No ROW_WID or INTEGRATION_ID'+'|'+ str(td_count) +'|'+ '' + '|' + 'No ROW_WID or INTEGRATION_ID in Table'
        logfile.write(log +'\n')
        continue
    
    ## ------------------- Code to Check empty tables -------------------------------##
    ## 1.Both Tables are Empty , then No data in source and Target Tables
    ## 2.Source Table is Empty, then No data fetched from source
    ## 3.Target Table is Empty, then No data fetched from Target
    ##---------------------------------------------------------------------------------------------------
    
    if df1.empty and df2.empty:
        #ora_count = len(ora_df)
        msg1 = tablename +'|'+'Both Tables Empty'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'No Data Found in both SRC and TGT Tables'
        logfile.write(msg1 +'\n')
        continue
    elif df1.empty:
        #ora_count = len(ora_df)
        msg2 = tablename +'|'+'No Data in SRC'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'No Data Found in Source Table'
        logfile.write(msg2 +'\n')
        continue
    elif df2.empty:
        #ora_count = len(ora_df)
        msg3 = tablename +'|'+'No Data in TGT'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'No Data Found in Target Table'
        logfile.write(msg3 +'\n')
        continue
    
            
    ## ------------------- Code to Compare Two Dataframes created above -------------------------------##
    ## 1.Check if exactly equals 
    ## 2.If not pass two dataframes to function for element wise comparison
    ## 3.Target Table is Empty
    ##---------------------------------------------------------------------------------------------------
    if td_count != ora_count:
        msg4 = tablename +'|'+'Count Mismatch'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'Count Mismatch'
        logfile.write(msg4 +'\n')
        continue
    elif df1.equals(df2):
        # Print("Data Matching for " + tablename + " ...Moving onto next Table")
        msg5 = tablename +'|'+'Data Matching'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'Data matching in Tables'
        logfile.write(msg5 +'\n')
        continue
    else:
        # For comparing empty cells values replasced with NaN in both Dataframes.
        df1 = df1.replace('',nan, regex=True) # Fields with values None in TD Dataframe will be replaces as NaN
        df2 = df2.fillna(value=nan)           # Fields with values '' in ORA Dataframe will be replaced as NaN
        results = compare_two_dfs(df1,df2)
        if results.shape[0] > 0:
            msg6 = tablename +'|'+'Data Mismatch'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'Data Mismatch Found in Tables'
            logfile.write(msg6 +'\n')
            results.to_csv(path + '\\data_mismatch_files\\' + tablename + '.csv')
        else:
            msg6 = tablename +'|'+'Data Matching'+'|'+ str(td_count) +'|'+ str(ora_count) + '|' + 'Data matching in Tables'
            logfile.write(msg6 +'\n')
            
## --------------------------------------------------------------------------------

# Code end time
end = datetime.datetime.now() 

# Script Timing Status
logfile.write('\nScript START Time: ' + str(start) + '\n'
'Script COMPLETED Time: ' + str(end) + '\n'
'Total execution time taken: ' + str(end - start) + '\n')

logfile.close()

