import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
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
from sap_sqls import *

class Global(object):
    root_path = 'C:/Users/qcells/Desktop/ATP/US_ATP_BI'
    driver= '{ODBC Driver 17 for SQL Server}'  
    atp_server = 'qcells-us-atp-db-server.database.windows.net'
    atp_database = 'us-qcells-atp-db'
    atp_username = 'qcells'
    atp_password = '{Asdqwe123!@#}'   
    qsp_server = '11.0.40.144'
    qsp_database = 'master'
    qsp_username = 'qspdev_user'
    qsp_password = '{QsxdRqsp123!#*}'   

class DB_Utils():
    def connect_qspdb(self):
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

    def connect_azuredb(self):
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
    
    def fetch_data(self, sql):
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchall()
            row = [list(i) for i in row]
            col_names = [item[0] for item in self.cursor.description]
            return pd.DataFrame(row, columns=col_names)
        except Exception as e:
            raise ConnectionError('ConnectionError: RPA FAILED TO FETCH DATA TO AZURE DB TABLES <br>', str(e))

    def insert_dataframe(self, tablename, df, writer = False):
        self.connect_azuredb()
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
    
    def insert_pd_tosql(self, tablename, df, writer = False):
        if writer == True:
            df['Writer'] = self.writer
        quoted = urllib.parse.quote_plus('''
                                        DRIVER={ODBC Driver 17 for SQL Server};
                                        SERVER=qcells-us-atp-db-server.database.windows.net;
                                        DATABASE=us-qcells-atp-db;
                                        UID=qcells;
                                        Pwd={Asdqwe123!@#}''')
        self.engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        print('[EVENT] INSERT INFO: \n', '\t TABLE NAME: ', tablename, '\n', '\t TABLE COLUMNS ARE BELOW', df.columns)
        df.to_sql(tablename, con=self.engine, if_exists='append', method='multi', index=False, chunksize=50)
        self.engine.dispose()

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

    def get_admin_address(self, RnRs = ['DEV']):
        if len(RnRs) == 1:
            RnRs.append('DUMMY')
        self.connect_azuredb()
        sql = f'''select MailAddr from ADMIN_INFO ai
                WHERE RnR in {tuple(RnRs)}'''    
        df_addr = self.fetch_data(sql)
        self.conn.close()
        return df_addr

    def read_qspdb(self, save_YN = False):
        self.excel_name = 'SAP_MASTER'
        self.connect_qspdb()        
        self.df_wh = self.fetch_data(sql_wh_code_sql)
        self.df_itemcode = self.fetch_data(sql_item_code_sql)
        if save_YN == True:
            self.df_wh.to_csv(Global.root_path + '/data/WH_Master.csv')
            self.df_itemcode.to_csv(Global.root_path + '/data/Item_Code_Master.csv')
            print('SAP MASTER SAVED.')
        self.conn.close()
        
    def initial_table(self):
        self.cursor.execute('delete from ITEM_CODE_MASTER_SAP')
        self.cursor.execute('delete from WAREHOUSE_INFO')
        self.conn.commit()
        self.conn.close()

    def insert_sap_data_to_db(self):
        print('[EVENT] STARTING TO SYNC SAP MASTER DATA [WAREHOUSE CODE MASTER] [ITEMCODE MASTER]')
        self.insert_dataframe('WAREHOUSE_INFO', self.df_wh)
        self.write_logs('WAREHOUSE_INFO', 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'SAP')
        self.insert_dataframe('ITEM_CODE_MASTER_SAP', self.df_itemcode)
        self.write_logs('ITEM_CODE_MASTER_SAP', 'PASS', datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'SAP')

    def update_sap_data(self):
        self.connect_azuredb()
        self.initial_table()
        self.insert_sap_data_to_db()