import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import math
import re
import time
import itertools
import os
import xlrd
import urllib
from datetime import timedelta
from sqlalchemy import create_engine
from tqdm import tqdm
import warnings
import sys
sys.path.append('../src')
from sap_sqls import *

class Global(object):
    root_path = 'C:/Users/qcells/Desktop/US_ATP_BI'
    driver= '{ODBC Driver 17 for SQL Server}'  
    atp_server = 'qcells-us-atp-db-server.database.windows.net'
    atp_database = 'us-qcells-atp-db'
    atp_username = 'qcells'
    atp_password = '{Asdqwe123!@#}'   
    qsp_server = '11.0.40.144'
    qsp_database = 'master'
    qsp_username = 'qspdev_user'
    qsp_password = '{QsxdRqsp123!#*}'   

class DB_Utils(): # Database basic functional class: connection, read, delete, commit
    def connect_qspdb(self): #functional block: connection 
        retry_cnt = 0
        while retry_cnt < 5:
            try:
                self.conn = pyodbc.connect('DRIVER='+Global.driver
                                        +';SERVER=tcp:'+Global.qsp_server
                                        +';PORT=1433;DATABASE='+Global.qsp_database
                                        +';UID='+Global.qsp_username
                                        +';Pwd='+Global.qsp_password)
                self.cursor = self.conn.cursor()
                break
            except Exception as e:
                print('AZURE DB CONNECTION RE-TRYING NOW COUNT: {} Times'.format(retry_cnt))
                e_msg = e
                retry_cnt = retry_cnt + 1
                time.sleep(5)
                pass
            if retry_cnt == 5:
                raise ConnectionError('ConnectionError: QSP DB CONNECTION FAILED <br>', str(e))
                break

    def connect_azuredb(self): #functional block: connection
        retry_cnt = 0
        while retry_cnt < 5:
            try:
                self.conn = pyodbc.connect('DRIVER='+Global.driver
                                        +';SERVER=tcp:'+Global.atp_server
                                        +';PORT=1433;DATABASE='+Global.atp_database
                                        +';UID='+Global.atp_username
                                        +';Pwd='+Global.atp_password
                                        +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
                self.cursor = self.conn.cursor()
                break
            except Exception as e:
                print('AZURE DB CONNECTION RE-TRYING NOW COUNT: {} Times'.format(retry_cnt))
                e_msg = e
                retry_cnt = retry_cnt + 1
                time.sleep(5)
                pass
            if retry_cnt == 5:
                raise ConnectionError('ConnectionError: AZURE DB CONNECTION FAILED <br>', str(e_msg))
                break
    
    def fetch_data(self, sql, db_conn='atp'): #functional block: read
        try:
            if db_conn == 'atp':
                self.connect_azuredb()
            else:
                self.connect_qspdb()
            self.cursor.execute(sql)
            row = self.cursor.fetchall()
            row = [list(i) for i in row]
            col_names = [item[0] for item in self.cursor.description]
            self.conn.close()
            return pd.DataFrame(row, columns=col_names)
        except Exception as e:
            raise ConnectionError('ConnectionError: RPA FAILED TO FETCH DATA TO AZURE DB TABLES <br>', str(e))

    def sql_execute_commit(self, sql, db_conn='atp'): #functional block: commit
        if db_conn == 'atp':
            self.connect_azuredb()
        else:
            self.connect_qspdb()

        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def insert_dataframe_to_db(self, tablename, df, writer = False, db_conn='atp'): #functional block: insert
        if db_conn == 'atp':
            self.connect_azuredb()
        else:
            self.connect_qspdb()
        self.error_check = []
        columns = df.columns.tolist()
        if writer == True:
            columns.append('Writer')
            df['Writer'] = self.writer
        cols_join = ', '.join(str(x) for x in columns)
        print('[EVENT] INSERT INFO: \n', '\t TABLE NAME: ', tablename, '\n', '\t TABLE COLUMNS ARE BELOW \n', columns)
        for i in tqdm(range(len(df))):
            try:
                time.sleep(0.01)
                sql = f'''INSERT INTO {tablename} ({cols_join}) VALUES {tuple(df[columns].values[i])}'''                
                self.cursor.execute(sql)
            except Exception as e:                
                print(e)
                print(sql)
                self.error_check.append(str(e))
                pass
        self.conn.commit()
        self.conn.close() 

    def insert_pd_tosql(self, tablename, df, writer = False): #functional block: insert
        if writer == True:
            df['Writer'] = self.writer
        quoted = urllib.parse.quote_plus('''
                                        DRIVER={ODBC Driver 17 for SQL Server};
                                        SERVER=qcells-us-atp-db-server.database.windows.net;
                                        DATABASE=us-qcells-atp-db;
                                        UID=qcells;
                                        Pwd={Asdqwe123!@#}''')
        self.engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted),  fast_executemany=True)
        print('[EVENT] INSERT INFO: \n', '\t TABLE NAME: ', tablename, '\n', '\t TABLE COLUMNS ARE BELOW', df.columns)
        chunksize = math.floor(2100/len(df.columns)) - 1
        df.to_sql(tablename, con=self.engine, if_exists='append', method='multi', index=False, chunksize=chunksize)
        self.engine.dispose()

class Log_Utils(DB_Utils): #Log tools
    def write_logs(self, FileName, Result, Received_time, ExcelType, MailSender='SYSTEM'):
        print('[EVENT] SAVED ATTACHMENTS LOG WROTTEN IN RPA_DOWNLOAD_LOGS TABLE')
        self.connect_azuredb()
        sql = f'''INSERT INTO RPA_DOWNLOAD_LOGS (ExcelName, Result, MailReceivedDate, ExcelType, MailSender)
                VALUES('{FileName}', '{Result}', '{Received_time}', '{ExcelType}', '{MailSender}');'''
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def write_error_logs(self, error_name, error_type, excel_name='SYSTEM', Mail_Sender='SYSTEM'):
        print('[EVENT] ERROR LOG WROTTEN IN ERROR_LOGS TABLE')
        self.connect_azuredb()
        sql = f'''INSERT INTO ERORR_LOGS (Error_Name, RPA_Type, Data_Type, Mail_Sender)
                VALUES('{error_name}', '{error_type}', '{excel_name}', '{Mail_Sender}');'''
        print(sql)
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def health_check_logs(self, System_Level, health_status = 1):
        print('[EVENT] HEALTH CHECK LOG WROTTEN IN SYSTEM_HEALTH_CHECK_LOGS TABLE')
        self.connect_azuredb()
        sql = f'''delete from SYSTEM_HEALTH_CHECK_LOGS WHERE System_Level = '{System_Level}'
                '''
        self.cursor.execute(sql)
        self.conn.commit()
        
        sql = f'''INSERT INTO SYSTEM_HEALTH_CHECK_LOGS (System_Level, Health_Check)
                VALUES('{System_Level}', {health_status});'''
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

class DB_Utils_Extra(Log_Utils): # higher level modules using DB_Utils
    def read_qspdb(self, save_YN = False):
        self.excel_name = 'SAP_MASTER'
        self.df_wh = self.fetch_data(sql_wh_code_sql, db_conn = 'qsp')
        self.df_itemcode = self.fetch_data(sql_item_code_sql, db_conn = 'qsp')
        if save_YN == True:
            self.df_wh.to_csv(Global.root_path + '/data/WH_Master.csv')
            self.df_itemcode.to_csv(Global.root_path + '/data/Item_Code_Master.csv')
            print('SAP MASTER SAVED.')
        
    def insert_sap_data_to_db(self):
        tables = ['ITEM_CODE_MASTER_SAP', 'WAREHOUSE_INFO']
        for table in tables:
            self.sql_execute_commit('delete from {}'.format(table))
            self.write_logs(table, 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'SAP')
        self.insert_dataframe_to_db('WAREHOUSE_INFO', self.df_wh)
        self.insert_dataframe_to_db('ITEM_CODE_MASTER_SAP', self.df_itemcode)
        print('[EVENT] STARTING TO SYNC SAP MASTER DATA [WAREHOUSE CODE MASTER] [ITEMCODE MASTER]')

    def check_isadmin(self): #will be -> sql to function
        sql = f'''select count(MailAddr) from ADMIN_INFO 
                WHERE MailAddr = '{self.sender_addr}' AND IsAdmin = 'Y' '''
        self.checkisadmin = self.fetch_data(sql)

    def meta_sheet_info(self): #will be -> sql to function
        sql = f'''select sheetlist, targetcolumns, datatype, NullCheck from SHEET_INFO
                where MailCategory = '{self.mail_category}' '''
        self.sheet_info = self.fetch_data(sql)
        self.target_sheet_list = list(set(self.sheet_info['sheetlist'].tolist()))

    def Check_file_is_updated(self): #will be -> sql to function
        sql = f'''
                SELECT COUNT(1) FROM {self.mail_category}
                WHERE Updated_Date = '{self.check_date_sql}' '''
        sql2 = f'''
                DELETE FROM {self.mail_category}
                WHERE Updated_Date = '{self.check_date_sql}' '''
        if int(self.fetch_data(sql).values[0][0]) > 1:
            print('[EVENT] {} DATASET HAS ALREADY SAVED TODAY. RPA IS INITIALIZING NOW'.format(self.mail_category))
            self.sql_execute_commit(sql2)
        else:
            pass

    def get_admin_address(self, RnRs = ['DEV']): #will be -> sql to function
        if len(RnRs) == 1:
            RnRs.append('DUMMY')
        sql = f'''select MailAddr from ADMIN_INFO ai
                WHERE RnR in {tuple(RnRs)}'''    
        df_addr = self.fetch_data(sql)
        return df_addr

    def check_batch_is_updated(self, table_name): #will be -> sql to function
        current_date = datetime.now().strftime('%Y-%m-%d')
        sql = f'''select count(1) from {table_name} with(nolock) where Batch_Date = '{current_date}' '''
        sql2 = f'''
                DELETE FROM {table_name}
                WHERE Batch_Date = '{current_date}' '''

        if int(self.fetch_data(sql).values[0][0]) > 1:
            print('[EVENT] {} HAS ALREADY SAVED TODAY. RPA IS INITIALIZING NOW'.format(table_name))
            self.sql_execute_commit(sql2)

    def updated_iscolumn_check(self):
        if 'Updated_date' in self.df.columns:
            self.check_date_sql = self.df['Updated_Date'].unique().values[0][0]
            print('[EVENT] EXCEL FILE HAS UPDATED_DATE COLUMN. UPDATE_DATE: {}'.format(self.check_date_sql))
        else:
            self.check_date_sql = datetime.now().strftime('%Y-%m-%d')
            self.df['Updated_Date'] = self.check_date_sql
            print('[EVENT] EXCEL FILE DOES NOT HAVE UPDATED_DATE COLUMN. RPA WILL CREATE. UPDATE_DATE: {}'.format(self.check_date_sql))

class Warning_Utils(DB_Utils_Extra):
    def inbound_qbis_pcs_check(self): 
        df = self.fetch_data('select * from QBIS_INVOICE_CHECK order by InvoiceNo, Item_Code, Updated_Date')
        if len(df) >= 1:
            df_html_merge = ''
            for InvoiceNo in df['InvoiceNo'].unique():
                for itemcode in df[df['InvoiceNo'] == InvoiceNo]['Item_Code'].unique():
                    df_html_merge = df_html_merge + df[(df['InvoiceNo'] == InvoiceNo) & (df['Item_Code'] == itemcode)].to_html(col_space='100%', index=False) + '<br>'        
           
            self.send_email('[CRITICAL][ATP] Inbound_Overseas PCS is greater than QBIS PCS'
                            ,'ERROR MESSAGE'
                            ,f'''Error: Inbound_Overseas PCS is greater than QBIS PCS. 
                                <br> Impact: In-take shipment will increase, thus increasing future available inventory amount.
                                <br> Instructions: There are {len(df)} discrepancy cases as below. Please double check the correct PCS and make sure the 3PL data is correct.
                                <br><br> *If QBIS data is incorrect, please coordinate with the relevant team and have them update the data to correct PCS.
                                '''                             
                            ,appendix = df_html_merge.replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'INBOUND_OVERSEA'
                            ,RnRs=['DEV', 'PLAN', '3PL_INBOUND']
                            # , destination = 'dany.shin@hanwha.com'
                            ) 

    def atp_data_check(self):        
        df = self.fetch_data('select * from QBIS_INVOICE_CHECK order by InvoiceNo, Item_Code, Updated_Date')
        if len(df) >= 1:
            df_html_merge = ''
            for InvoiceNo in df['InvoiceNo'].unique():
                for itemcode in df[df['InvoiceNo'] == InvoiceNo]['Item_Code'].unique():
                    df_html_merge = df_html_merge + df[(df['InvoiceNo'] == InvoiceNo) & (df['Item_Code'] == itemcode)].to_html(col_space='100%', index=False) + '<br>'        
           
            self.send_email('[CRITICAL][ATP] Inbound_Overseas PCS is greater than QBIS PCS'
                            ,'ERROR MESSAGE'
                            ,f'''<strong>Error</strong>: Inbound_Overseas PCS is greater than QBIS PCS. 
                                <br> <strong>Impact</strong>: In-take shipment will increase, thus increasing future available inventory amount.
                                <br><br> <strong>Instructions</strong>: There are {len(df)} discrepancy cases as below. Please double check the correct PCS and make sure the 3PL data is correct.'''                             
                            ,appendix = df_html_merge.replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'INBOUND_OVERSEA'
                            ,RnRs=['DEV', 'PLAN', '3PL_INBOUND']
                            ,critical = True
                            # , destination = 'dany.shin@hanwha.com'
                            ) 

        df = self.fetch_data('select * from OSR_CPO_PCS_CHECK')
        if len(df) >= 1:
            self.send_email('[ATP] Outbound PCS is greater than OSR PCS'
                            ,'ERROR MESSAGE'
                            ,f'''<strong>Error</strong>: Outbound PCS is greater than OSR PCS. 
                                <br><br> <strong>Impact</strong>: Shipped PCS will be greater than hard allocation PCS, thus decreasing future available inventory amount.
                                <br><br> <strong>Instructions</strong>: Please double check the below cases on both OSR & Outbound reports and fix the incorrect PCS count.
                                ''' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['PLAN','DEV', 'OSR', '3PL_OUTBOUND']
                            # , destination = 'dany.shin@hanwha.com'
                            
                            )

        df = self.fetch_data('select * from OSR_CPO_MATCH_CHECK')        
        if len(df) >= 1:
            self.send_email('[CRITICAL][ATP] CPO# or item code discrepancies between Outbound and OSR'
                            ,'ERROR MESSAGE'
                            ,'''<strong>Error</strong>: (1) CPO# on Outbound not found on OSR, or/and 
                                <br> (2) CPO# on Outbound & OSR the same, but item codes different
                                <br><br> <strong>Impact</strong>: CPO on Outbound (Shipped) and potentially the same CPO on OSR (but in different CPO#) will both be counted  
                                <br> (Double counted), thus decreasing future available inventory amount.
                                <br><br> <strong>Instructions</strong>: Please review the below CPO#s and update to correct CPO# on OSR or Outbound as necessary, as well as correct item codes.
                                <br><br> **This is data found on Outbound report**''' 
                            ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                            ,warning = True
                            ,excel_name = 'ORDER_STATUS_REPORT'
                            ,RnRs=['PLAN','DEV', 'OSR', '3PL_OUTBOUND']
                            # , destination = 'dany.shin@hanwha.com'
                            
                            ,critical=True)
    
    def atp_join_check(self, table_name, nullcheck_total_result, nullcheck_csv_path, RnRs):
        self.send_email('[ATP] {} item codes & warehouses not on SAP master'.format(table_name)
                        ,'ERROR MESSAGE'
                        ,f'''
                            <strong>Error</strong>: Item codes & warehouses on the {table_name} report not on SAP master causing join error with product names & warehouses.
                            <br> {nullcheck_total_result} <br>
                            <br> <strong>Impact</strong>: Total available inventory amount is not affected, but when filtering by item codes, product names, or warehouses, the available inventory may be decreased.
                            <br><br> <strong>Instructions</strong>: Please check the attached cases and  
                            <br> (1) add any blank item codes or WH names, or/and 
                            <br> (2) fix incorrect item codes or WH names, or/and 
                            <br> (3) if new item codes, add them to SAP'''
                        , attachment_path = nullcheck_csv_path
                        , warning = True
                        , excel_name = self.excel_name
                        # , destination = 'dany.shin@hanwha.com'
                        , RnRs = RnRs
                        )

        # df = self.fetch_data('select * from OSR_CPO_ETD_CHECK')
        # if len(df) >= 1:
        #     self.send_email('[ATP] OSR allocation CW in future but outbound already delivered'
        #                     ,'ERROR MESSAGE'
        #                     ,''' Impact: Both hard allocation and CPO shipped will be counted (Double counted), thus decreasing future available inventory amount.
        #                         <br>Instructions: Please update the OSR ALLOC_CW to match Outbount ATD.'''
        #                     ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
        #                     ,warning = True
        #                     ,excel_name = 'ORDER_STATUS_REPORT'
        #                     ,RnRs=['OSR', 'PLAN', 'DEV', '3PL_OUTBOUND']
        #                     )


        # df = self.fetch_data('select * from OSR_CPO_ATD_CHECK')
        # if len(df) >= 1:
        #     self.send_email('[ATP] OSR allocation CW in past, but outbound not delivered'
        #                     ,'ERROR MESSAGE'
        #                     ,f'''
        #                         Error: OSR allocation CW in past, but outbound not delivered 
        #                         <br> Impact: Hard-allocations will remain in the past so no impact on ATP (These will be moved to shipped once ETD is added)
        #                         <br> Instructions: There are {len(df)} POs that have passed the original requested delivery dates. 
        #                         <br> Please use this list for your reference.''' 
        #                     ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
        #                     ,warning = True
        #                     ,excel_name = 'ORDER_STATUS_REPORT'
        #                     ,RnRs=['OSR', 'PLAN','DEV', '3PL_OUTBOUND']
        #                     # , destination = 'dany.shin@hanwha.com'
        #                     )

        # df = self.fetch_data('select * from ALLOCATION_NEW_ITEM_CHECK')        
        # if len(df) >= 1:
        #     self.send_email('[RPA WARNING] QBIS ALLOCATION PLAN NEW ITEMCODE CREATED'
        #                     ,'ERROR MESSAGE'
        #                     ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> QBIS ALLOCATION PLAN NEW ITEMCODE CREATED' 
        #                     ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
        #                     ,warning = True
        #                     ,excel_name = 'ORDER_STATUS_REPORT'
        #                     ,RnRs=['PLAN','DEV', '3PL_INBOUND']
        #                     # , destination = 'dany.shin@hanwha.com'
        #                     )

class Check_Utils(DB_Utils_Extra):    
    def atp_data_null_check(self, table_name, join_check_column_list, RnRs):
        join_check_column_list_upper = list(map(str.upper,join_check_column_list))
        nullcheck_cols = [col for col in self.df.columns if col.upper() in join_check_column_list_upper]
        nullcheck_total_result = []
        if len(nullcheck_cols) > 0:
            for ncols in nullcheck_cols:
                nullcheck_result = self.df[ncols].isnull().sum() + len(self.df[self.df[ncols] == ''])
                if nullcheck_result > 0:
                    nullcheck_total_result.append([ncols, nullcheck_result])
                print('[EVENT] {} BATCH VIEW, {} COLUMN NULL CHECKING RESULT IS : {}'.format(table_name, ncols, nullcheck_result))

        if len(nullcheck_total_result) > 0:
            for null_col in nullcheck_total_result:
                null_col = null_col[0]
                df_error = self.df[(self.df[null_col].isna()) | (self.df[null_col] == '')].reset_index(drop = True)
                if table_name == 'ATP_OUTBOUND':
                    table_name = 'OSR & Outbound'
                    df_error.rename(columns = {'WH_Code' : 'WH_Location'}, inplace = True)
                df_error.to_csv(Global.root_path + '/data/dummy/joinError_{}.csv'.format(null_col), encoding='utf-8-sig',  index = None)
            nullcheck_csv_path = [Global.root_path + '/data/dummy/joinError_{}.csv'.format(i[0]) for i in nullcheck_total_result]
            nullcheck_total_result = ['  - {} : {} rows'.format(i[0], i[1]) for i in nullcheck_total_result]
            nullcheck_total_result = ' <br> '.join(nullcheck_total_result)
            nullcheck_total_result = nullcheck_total_result.replace('Product_Name' , 'Item_Code')
            print('[WARNING] THIS ATTACHMENT HAS AN NULL VALUES.')
            self.atp_join_check(table_name, nullcheck_total_result, nullcheck_csv_path, RnRs)

    def data_null_check(self):
        print("[EVENT] RPA ETL {} FILE iS ABOUT TO NULL-CHECK.".format(self.mail_category))        
        nullcheck_total_result = []
        nullcheck_cols = self.sheet_info[self.sheet_info['NullCheck'] == 'Y']['targetcolumns'].tolist()
        print('[EVENT] NULL CHECK COLUMNS iS BELOW: \n ', nullcheck_cols)
        
        if len(nullcheck_cols) > 0:
            for ncols in nullcheck_cols:
                nullcheck_result = self.df[ncols].isnull().sum() + len(self.df[self.df[ncols] == ''])
                if nullcheck_result > 1:
                    print('[EVENT] {} FILE, {} COLUMN NULL CHECKING RESULT IS : {}'.format(self.mail_category, ncols, nullcheck_result))
                    nullcheck_total_result.append([ncols, nullcheck_result])
                
            if len(nullcheck_total_result) > 0:
                nullcheck_total_result = ['{} : {} ROWS MISSED'.format(i[0], i[1]) for i in nullcheck_total_result]
                nullcheck_total_result = ' <br> '.join(nullcheck_total_result)
                print('[WARNING] THIS ATTACHMENT HAS AN NULL VALUES.')
                content_body = 'DataNullWarning: {} FILE, NULL CHECKING RESULT : <br> {}'.format(self.mail_category, nullcheck_total_result)
                self.write_error_logs(error_name = content_body, error_type = content_body.split(':')[0], excel_name = self.mail_category, 
                                        Mail_Sender = self.writer)
