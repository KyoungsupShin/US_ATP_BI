import pandas as pd
import numpy as np
from datetime import datetime
import win32com
import win32com.client
import re
import time
import itertools
import os
from difflib import SequenceMatcher
import xlrd
import urllib
from datetime import timedelta
from sqlalchemy import create_engine
from tqdm import tqdm
import warnings
from db_conns import Global, DB_Utils
from data_manipulation import Manipluations

class Master_Reset(DB_Utils):
    def check_isadmin(self):
        self.connect_azuredb()
        sql = f'''select count(MailAddr) from ADMIN_INFO 
                WHERE MailAddr = '{self.sender_addr}' AND IsAdmin = 'Y' 
                '''
        self.checkisadmin = self.fetch_data(sql)
        self.conn.close()

    def reset_master_tables(self):
        self.connect_azuredb()
        try:
            self.master_df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl')
            for sheet in self.master_df.sheet_names:
                if (sheet != 'US_CITY_CODE') and (sheet != 'POWER_CLASS_DIST'):
                    sql = 'DELETE FROM {}'.format(sheet)
                    self.cursor.execute(sql)
                    self.conn.commit()
        except Exception as e :
            raise ValueError('MasterResetError: WHILE RESETTING MASTER TABLES, THERE WAS AN ERROR. <br>', str(e))

    def push_data(self):
        try:
            print('[EVENT] NEW MASTER DATA IS BEING SAVED !')
            for sheet_name in self.master_df.sheet_names:
                if (sheet_name != 'US_CITY_CODE') and (sheet_name != 'POWER_CLASS_DIST'):
                    df = pd.ExcelFile(self.file_path,engine='openpyxl').parse(sheet_name)
                    df = df.dropna(how='all', axis='columns')
                    df.dropna(inplace = True, how = 'all')
                    df.fillna('', inplace = True)
                    if sheet_name == 'POWER_CLASS_DIST':
                        df = self.unpivot_powerclassdist(df)
                    if sheet_name == 'CW_CALENDER':
                        df['Friday'] = df['Friday'].astype('str')
                    if sheet_name == 'TARIFF_INFO':
                        df = df = df.melt(id_vars = ['Year', 'Month', 'YYYYMM'], value_vars=df.columns[3:]).rename(columns = {'variable' : 'FactoryCd', 
                                                                                                                                'value' : 'TariffCd'})
                    self.insert_dataframe(sheet_name, df)
            print('\n')
        except Exception as e:
            raise ValueError('MasterResetError: WHILE PUSHING DATA TO MASTER TABLES, THERE WAS AN ERROR. <br>', str(e))

    def unpivot_powerclassdist(self, df):
        power_df = pd.melt(df, id_vars=['FactoryCd','Product_Name', 'DummyID', 'SAP_UNIQUE_CODE', 'Power_Class', 'Segment'], value_vars=df.columns[6:])
        power_df = power_df.rename(columns = {'variable' : 'YYYYMM', 'value' : 'Distribution'})
        power_df.YYYYMM = power_df.YYYYMM.astype('str')
        return power_df
        
    def master_reset_main(self, sender_addr, file_path = Global.root_path+'/data/master.xlsx'): 
        self.file_path = file_path
        self.reset_master_tables()
        self.push_data()

class ETL_Utils(DB_Utils, Manipluations):
    def meta_sheet_info(self):
        self.connect_azuredb()
        sql = f'''select sheetlist, targetcolumns, datatype, NullCheck from SHEET_INFO
                where MailCategory = '{self.mail_category}' '''
        self.sheet_info = self.fetch_data(sql)
        self.target_sheet_list = list(set(self.sheet_info['sheetlist'].tolist()))
        self.conn.close()

    def read_rpa_excel_file(self, path, engine, sheet_name):
        time.sleep(1)
        skiprows = 0
        while True:
            try:
                print('[WARNING] {} / {} EXCEL FILE SKIP ROWS: {}'.format(path, sheet_name, skiprows))
                self.df = pd.read_excel(path
                                    , engine=engine
                                    , sheet_name = sheet_name
                                    , skiprows= skiprows)
                break
            except Exception as e:
                if str(e).split(' ')[0][1:] == 'Worksheet':
                    sheet_name = 0
                else:
                    skiprows = skiprows + 1
                    if skiprows > 5:
                        raise ValueError('ExtractDataError: PLEASE CHECK OUT THE SHEET NAME OR COLUMN SKIP LINE IN THIS EXCEL FILE : {} '.format(path)) 
                    print(e)
                pass
        cols = [col for col in self.df.columns if col is not None]
        self.df = self.df[cols]
        cols = [col for col in self.df.columns if col[:7] != 'Unnamed']
        self.df = self.df[cols]
        self.df.columns = [col.strip() for col in self.df.columns]
        if self.mail_category == 'ORDER_STATUS_REPORT':
            self.df.columns = [col.split('(')[0].rstrip() for col in self.df.columns]
            self.df.rename(columns = {'Task Name' : 'CustomerPO_Num'
                                , 'Task ID' : 'CR_Num'}, inplace = True)
        self.df = self.df[self.sheet_info.targetcolumns.tolist()]
        return self.df

    def read_excel(self, path = Global.root_path):
        try:
            print('[EVENT] EXCEL FILE IS READING. \n PATH: {}'.format(path))
            if path.split('.')[-1].lower() == 'xlsb':
                self.df = self.read_rpa_excel_file(path,engine='pyxlsb', 
                                        sheet_name=self.target_sheet_list[0])
            elif path.split('.')[-1].lower() == 'csv':
                self.df = pd.read_csv(path, encoding = 'cp949')                    
            else:
                self.df = self.read_rpa_excel_file(path,engine='openpyxl', 
                                        sheet_name=self.target_sheet_list[0])        
            
            if 'Updated_date' in self.df.columns:
                self.check_date_sql = self.df['Updated_Date'].unique().values[0][0]
                print('[EVENT] EXCEL FILE HAS UPDATED_DATE COLUMN. UPDATE_DATE: {}'.format(self.check_date_sql))
            else:
                self.check_date_sql = datetime.now().strftime('%Y-%m-%d')
                self.df['Updated_Date'] = self.check_date_sql
                print('[EVENT] EXCEL FILE DOES NOT HAVE UPDATED_DATE COLUMN. RPA WILL CREATE. UPDATE_DATE: {}'.format(self.check_date_sql))
        except Exception as e:
            raise KeyError('ExtractDataError: WHILE READING EXCEL FILE ERROR OCCURED <br>' + str(e))

    def Check_file_is_updated(self):        
        self.connect_azuredb()
        sql = f'''
                SELECT COUNT(1) FROM {self.mail_category}
                WHERE Updated_Date = '{self.check_date_sql}' '''
        sql2 = f'''
                DELETE FROM {self.mail_category}
                WHERE Updated_Date = '{self.check_date_sql}' '''

        if int(self.fetch_data(sql).values[0][0]) > 1:
            print('[EVENT] {} DATASET HAS ALREADY SAVED TODAY. RPA IS INITIALIZING NOW'.format(self.mail_category))
            self.cursor.execute(sql2)
            self.conn.commit() 
        else:
            pass
        self.conn.close()

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

    def read_date(self, date):
        return xlrd.xldate.xldate_as_datetime(date, 0)

    def data_prod_plan_clean(self):
        print('[EVENT] GA PRODUCTION PLAN ETL ON PROGRESS. IT NEEDS TO BE UN-PIVOT.')
        self.df = pd.read_excel(self.saved_path
                                    , engine='pyxlsb'
                                    , sheet_name = 'Revised_Detail PSI (item code)'
                                    , skiprows= 4)
        self.df = self.df.dropna(how='all', axis='columns')
        self.df = self.df[(self.df['Curr/Futu Prod.'] !='X')]
        self.df = self.df[self.df['MODEL'].notna()]
        self.df = self.df[self.df['category'] == 'W/H IN (생산)']
        self.df = self.df.drop(['category', 'Grade','Cell type', 'J.Box', 'Curr/Futu Prod.'], axis = 1)
        self.df = self.df.reset_index(drop = True)
        self.df = self.df.melt(id_vars = ['MODEL', 'Power', 'Item code'])
        self.df['variable']= pd.to_datetime(self.df['variable'].apply(self.read_date), errors='coerce')
        self.df = self.df.dropna()
        self.df['variable'] = self.df['variable'].apply(lambda x:self.yyyymmdd_datetime(str(x)))
        self.df = self.df.rename(columns = {'MODEL' : 'ProductName',
                                            'Power' : 'Power_Class',
                                            'Item code' : 'Item_Code',
                                            'variable' : 'Product_Plan_Date', 
                                            'value' : 'MW'})

    def clean_data_text(self, func):
        try:
            target_columns_text = self.sheet_info[self.sheet_info['datatype'] == 'TEXT']['targetcolumns'].tolist()
            for col in target_columns_text:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[text] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_int(self, func):
        try:
            target_columns_int = self.sheet_info[self.sheet_info['datatype'] == 'INT']['targetcolumns'].tolist()
            for col in target_columns_int:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[int] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_float(self, func):
        try:
            target_columns_float = self.sheet_info[self.sheet_info['datatype'] == 'FLOAT']['targetcolumns'].tolist()
            for col in target_columns_float:
                self.df[col] = self.df[col].apply(lambda x:func(x))
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[float] <br>'.format(self.saved_path, col) + str(e))

    def clean_data_datetime(self, func):
        self.connect_azuredb()
        try:
            target_columns_datetime = self.sheet_info[self.sheet_info['datatype'] == 'DATETIME']['targetcolumns'].tolist()
            for col in target_columns_datetime:
                self.df[col] = self.df[col].apply(lambda x:func(str(x)))        
        except Exception as e:
            raise KeyError('TransformDataError: {} file {} column[datetime] <br>'.format(self.saved_path, col) + str(e))

    def validate_dataframe(self):
        self.Check_file_is_updated()
        self.data_null_check()
        self.insert_dataframe(self.mail_category, self.df, writer = True)
        self.osr_etd_check()

    def osr_etd_check(self):
        if self.mail_category == 'ORDER_STATUS_REPORT':
            eu = Email_Utils()
            eu.connect_azuredb()
            df = eu.fetch_data('select * from OSR_CPO_ETD_CHECK')
            df.to_csv('../data/dummy/error.csv')
            if len(df) >= 1:
                eu.send_email('[RPA WARNING] OUTBOUND CPO DELIVERED-DONE BEFORE ALLOCATION CW'
                                ,'ERROR MESSAGE'
                                ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND CPO DELIVERED-DONE BEFORE ALLOCATION CW' 
                                ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                                ,warning = True
                                ,excel_name = 'ORDER_STATUS_REPORT'
                                ,RnRs=['OSR', 'PLAN', 'DEV']
                                )
            eu.connect_azuredb()
            df = eu.fetch_data('select * from OSR_CPO_ATD_CHECK')
            df.to_csv('../data/dummy/error.csv')
            if len(df) >= 1:
                eu.send_email('[RPA WARNING] OUTBOUND NOT-DELIVERED AFTER ALLOCATION CW'
                                ,'ERROR MESSAGE'
                                ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND CPO NOT-DELIVERED AFTER ALLOCATION CW' 
                                ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                                ,warning = True
                                ,excel_name = 'ORDER_STATUS_REPORT'
                                ,RnRs=['OSR', 'PLAN','DEV']
                                )
            eu.connect_azuredb()
            df = eu.fetch_data('select * from OSR_CPO_PCS_CHECK')
            df.to_csv('../data/dummy/error.csv')
            if len(df) >= 1:
                eu.send_email('[RPA WARNING] OUTBOUND PCS IS GREATER THEN OSR PCS'
                                ,'ERROR MESSAGE'
                                ,'ValueWarning: DATE VALIDATION CHECKING RESULT, <br> OUTBOUND PCS IS GREATER THEN OSR PCS' 
                                ,appendix = df.to_html(index=False).replace('<td>', '<td align="center">')
                                ,warning = True
                                ,excel_name = 'ORDER_STATUS_REPORT'
                                ,RnRs=['PLAN','DEV', '3PL']
                                )

            del eu

class ETL_Pipelines(ETL_Utils, DB_Utils):
    def __init__(self, saved_path='', mail_category='', check_date = datetime.now().strftime('%Y-%m-%d'), writer = 'SYSTEM', manual = False):
        self.saved_path = saved_path
        self.mail_category = mail_category
        self.check_date_sql = check_date # => dataframe의 updated_date를 참조
        self.writer = writer
        if manual == False:
            self.ETL_Basic_Process()

    def ETL_Basic_Process(self):
        self.meta_sheet_info()
        if self.mail_category == 'GA_PROD_PLAN':
            self.data_prod_plan_clean()
        else:
            self.read_excel(path = self.saved_path)
            self.clean_data_text(self.data_text)
            self.clean_data_int(self.data_int)
            self.clean_data_int(int)
            self.clean_data_float(self.data_float)
            self.clean_data_float(float)
            self.clean_data_datetime(self.yyyymmdd_datetime)
        self.validate_dataframe()

class Email_Utils(Master_Reset, ETL_Utils):
    def __init__(self, mail_receivers =  "digital_scm@us.q-cells.com"):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5

    def send_email(self, mail_title, content_title, content_body, destination = None, appendix = '', attachment_path='', warning=False, excel_name='SYSTEM', RnRs = ['DEV']):    
        try:
            with open("../utils/template.html", "r", encoding='utf-8') as f:
                text= f.read()
            text = text.replace('RPA-TITLE' , content_title)        
            text = text.replace('RPA-CONTENTS', content_body + appendix)        
            Txoutlook = self.outlook.CreateItem(0)

            if destination is None:
                Txoutlook.To = ';'.join(self.get_admin_address(RnRs).stack().tolist()) # Destination mail address
                print('[EVENT] SENT MAIL TO {}'.format(';'.join(self.get_admin_address(RnRs).stack().tolist())))
            else:           
                Txoutlook.To = str(destination) # Destination mail address           
                print('[EVENT] SENT MAIL TO {}'.format(str(destination)))
            Txoutlook.Subject = mail_title
            Txoutlook.HTMLBody = f"""{text}"""
            if attachment_path:
                Txoutlook.Attachments.Add(attachment_path)
            Txoutlook.Send()
            
            if warning == True:
                self.write_error_logs(error_name = content_body, error_type = content_body.split(':')[0], excel_name = excel_name)
        except Exception as e:
            raise KeyError('EmailError: THERE WAS AN ERROR DURING SENDING MAIL. ERROR MESSGAE IS: ' + str(e))

    def sender_mailaddr_extract(self, i):
        if i.SenderEmailType=='EX':
            if i.Sender.GetExchangeUser() != None:
                return i.Sender.GetExchangeUser().PrimarySmtpAddress
            else:
                return i.Sender.GetExchangeDistributionList().PrimarySmtpAddress
        else:
            return i.SenderEmailAddress

    def access_mailbox(self):
        try:
            self.inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
        except:
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.Rxoutlook = self.outlook.GetNamespace("MAPI")    
            self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)  
            self.inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
            pass
        print('[ITERATION] {} TOTAL E-MAIL FILED: {}'.format(datetime.now().strftime('%Y-%m-%d'),len(self.inbox.items)))
    
    def mail_target_extension_filter(self, download_filetype):
        atts = []
        for filetype in range(len(download_filetype)):
            atts.append([att for att in self.i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
        self.atts = list(itertools.chain(*atts))                
        self.sender_addr = self.sender_mailaddr_extract(self.i)

    def category_replace(self, mail_category_parse):
        if mail_category_parse.lower() == 'inbound_oversea':
            mail_category_parse = 'INBOUND_OVERSEA'
        elif mail_category_parse.lower() == 'inbound_domestic':
            mail_category_parse = 'INBOUND_DOMESTIC'
        elif mail_category_parse.lower() == 'outbound':
            mail_category_parse = 'OUT_BOUND'
        elif mail_category_parse.lower() == 'onhand':
            mail_category_parse = 'ON_HAND'
        elif mail_category_parse.lower() == 'osr':
            mail_category_parse = 'ORDER_STATUS_REPORT'
        elif mail_category_parse.lower() == 'ga_prod_plan':
            mail_category_parse = 'GA_PROD_PLAN'
        else:
            mail_category_parse = 'None'
        return mail_category_parse

    def extract_request_update_mail_info(self): #used only  request, update date
        try:
            title_parsed = self.i.subject.lower().split('/')
            mail_category_parse = re.sub(r'[^a-zA-Z]', '', title_parsed[1])
            self.mail_category_parse = self.category_replace(mail_category_parse)
            self.Req_date = datetime.now().strftime('%Y-%m-%d')
            if len(title_parsed) >= 3:
                if len(title_parsed[2]) == 8:
                    self.Req_date = self.yyyymmdd_datetime(title_parsed[2])                
        except Exception as e:
            raise ValueError('EmailError:THERE IS SOMETHING WRONG IN MAIL TITLE FOR PARSING CATEGORIES. <br>', str(e))

    def invalidate_whitelist(self, mail_title, attch_name, mail_domain):
        self.connect_azuredb()
        sql = 'select Mailtitle, Domain, ExcelName, MailCategory from MAIL_LIST ml'
        whitelist_dict = self.fetch_data(sql)
        whitelist_dict = whitelist_dict.to_dict('records')
        mail_domain = mail_domain.split('@')[-1]
        similarity = 0
        similarity_attach = 0
        isdomainsame = False
        mail_category_parse = 'None'
        for idx, whitelist in enumerate(whitelist_dict):
            if similarity <= SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio():
                similarity = SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio()
                max_ssim_title = whitelist['Mailtitle']
                max_ssim_domian = whitelist_dict[idx]['Domain']     
                     
                for idx, whitelist in enumerate(whitelist_dict):
                    if similarity_attach <= SequenceMatcher(None, whitelist['ExcelName'], attch_name).ratio():
                        similarity_attach = SequenceMatcher(None, whitelist['ExcelName'], attch_name).ratio()
                        max_ssim_excel = whitelist['ExcelName']
                        mail_category_parse = whitelist['MailCategory']
                        self.mail_category_parse= self.category_replace(mail_category_parse)                
                if (mail_domain == max_ssim_domian) & (similarity > self.similarity_threshold) & (similarity_attach > self.similarity_threshold):
                    isdomainsame = True
                    break
                else:
                    isdomainsame = False
        return similarity, isdomainsame, self.mail_category_parse

    def save_attachments(self, att, i, save_date = datetime.now().strftime('%Y-%m-%d')):
        print('[EVENT] SAVED ATTACHMENTS: {}'.format(att.FileName))
        saved_dir = Global.root_path + '/data/' + save_date
        saved_path = saved_dir + '/' + att.FileName
        os.makedirs(saved_dir, exist_ok = True)
        att.SaveAsFile(saved_path)
        ETL_Pipelines(saved_path, self.mail_category_parse, save_date, writer = self.sender_addr.split('@')[0])
        self.write_logs(self.att.FileName, 'PASS', self.i.SentOn.strftime('%Y-%m-%d %H:%M:%S'), self.mail_category_parse, self.sender_addr.split('@')[0])

