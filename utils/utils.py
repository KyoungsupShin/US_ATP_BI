import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import win32com
import win32com.client
import re
import threading
import time
import itertools
from difflib import SequenceMatcher
import os
import glob

class Global(object):
    root_path = 'C:/Users/qcells/Desktop/ATP/US_ATP_BI'

class DB_Utils():
    def connect_azuredb(self):
        server = 'qcells-us-atp-db-server.database.windows.net'
        database = 'us-qcells-atp-db'
        username = 'qcells'
        password = '{Asdqwe123!@#}'   
        driver= '{ODBC Driver 17 for SQL Server}'  
        self.conn = pyodbc.connect('DRIVER='+driver
                                +';SERVER=tcp:'+server
                                +';PORT=1433;DATABASE='+database
                                +';UID='+username
                                +';Pwd='+password
                                +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        self.cursor = self.conn.cursor()
    
    def fetch_data(self, sql):
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        row = [list(i) for i in row]
        col_names = [item[0] for item in self.cursor.description]
        return pd.DataFrame(row, columns=col_names)

    def insert_dataframe(self, tablename, df):
        self.error_check = []
        cols = ', '.join(str(x) for x in df.columns)
        print('[EVENT] INSERT INFO: \n', tablename, cols)
        for i in range(len(df)):
            try:
                time.sleep(0.01)
                sql = f'''INSERT INTO {tablename} ({cols}) VALUES {tuple(df[df.columns].values[i])}'''
                self.cursor.execute(sql)
            except Exception as e:                
                print(e)
                print(sql)
                self.error_check.append(str(e))
                pass
        self.conn.commit() 

class ETL_Utils(DB_Utils):
    def __init__(self, excel_name):
        self.excel_name = excel_name
        self.connect_azuredb()
        self.path = '../data/dummy/{}.xlsx'.format(self.excel_name)

    def meta_sheet_info(self):
        sql = f'''select sheetlist, targetcolumns, datatype from SHEET_INFO
                where ExcelName = '{self.excel_name}' and BackUpYN = 'Y'
                '''
        self.sheet_info = self.fetch_data(sql)
        self.target_sheet_list = list(set(self.sheet_info['sheetlist'].tolist()))

    def read_excel(self):
        if self.path[-4:] == 'xlsb':
            df_obj = pd.ExcelFile(self.path,engine='pyxlsb')
        else:
            df_obj = pd.ExcelFile(self.path,engine='openpyxl')
        
        df = df_obj.parse(sheet_name=self.target_sheet_list[0])
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        self.df = df.dropna(how = 'all')

    def yyyymmdd_datetime(self, x):
        if len(x) == 0: #fully string data ex) TBD
            return ''
        
        if pd.isna(x): # nan value filter
            return '' 
        
        try:
            if pd.isnull(np.datetime64(x)): #NAT value filter
                return ''    
        except:
            pass

        x = str(x)
        removal_string = ['/', '.', '-', '_', ' ']    
        for rs in removal_string:
            x = x.replace(rs, '')
        if len(x) == 6:
            x = x + '01'
        else:
            x = x[:8]
        
        try:
            return datetime.strftime(datetime.strptime(x, '%Y%m%d'), '%Y-%m-%d')
        except:
            try:
                return datetime.strftime(datetime.strptime(x, '%m%d%Y'), '%Y-%m-%d')
            except:
                return ''
                pass
            pass

    def data_int(self, x):
        if pd.isna(x):
            return 0
        elif (type(x) != int) or (type(x) != float):
            return 0
        else:
            return int(x)

    def data_float(self, x):
        if type(x) == str:
            x = re.findall(r'\d+', x)
            if len(x) == 0:
                return 0
            else:
                return float(x)
        elif type(x) == int:
            return float(x)
        else: 
            return 0
    
    def data_text(self, x):
        if pd.isna(x):
            return ''
        else:
            x = str(x)
            x = x.replace("'", "")

            if len(x) > 150:
                return x[:100]
            else:
                return x
    
    def clean_data_text(self, func):
        target_columns_text = self.sheet_info[self.sheet_info['datatype'] == 'TEXT']['targetcolumns'].tolist()
        for col in target_columns_text:
            self.df[col] = self.df[col].apply(lambda x:func(x))

    def clean_data_int(self, func):
        target_columns_int = self.sheet_info[self.sheet_info['datatype'] == 'INT']['targetcolumns'].tolist()
        for col in target_columns_int:
            self.df[col] = self.df[col].apply(lambda x:func(x))

    def clean_data_float(self, func):
        target_columns_float = self.sheet_info[self.sheet_info['datatype'] == 'FLOAT']['targetcolumns'].tolist()
        for col in target_columns_float:
            self.df[col] = self.df[col].apply(lambda x:func(x))

    def clean_data_datetime(self, func):
        target_columns_datetime = self.sheet_info[self.sheet_info['datatype'] == 'DATETIME']['targetcolumns'].tolist()
        for col in target_columns_datetime:
            self.df[col] = self.df[col].apply(lambda x:func(str(x)))
        self.insert_dataframe(self.excel_name, self.df)
        self.conn.close()

class Master_Reset(DB_Utils):
    def check_isadmin(self, sender_addr):
        self.connect_azuredb()
        sql = f'''select count(MailAddr) from ADMIN_INFO 
                WHERE MailAddr = '{sender_addr}' '''
        self.checkisadmin = self.fetch_data(sql)
        self.conn.close()

    def reset_master_tables(self):
        self.master_df = pd.ExcelFile(Global.root_path + '/data/master.xlsx',engine='openpyxl')
        for sheet in self.master_df.sheet_names:
            if sheet != 'US_CITY_CODE':
                sql = 'DELETE FROM {}'.format(sheet)
                self.cursor.execute(sql)
        self.conn.commit()

    def push_data(self):
        print('[EVENT] NEW MASTER DATA IS BEING SAVED !')
        for sheet_name in self.master_df.sheet_names:
            if sheet_name != 'US_CITY_CODE':
                df = pd.ExcelFile(self.file_path,engine='openpyxl').parse(sheet_name)
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                df.dropna(inplace = True, how = 'all')
                df.fillna('', inplace = True)
                self.insert_dataframe(sheet_name, df)
        print('\n')

    def master_reset_main(self, sender_addr, file_path = Global.root_path+'/data/master.xlsx'): 
        self.file_path = file_path
        self.reset_master_tables()
        self.push_data()

class ETL_Pipelines(DB_Utils):
    def __init__(self, saved_path, mail_category, check_date = datetime.now().strftime('%Y-%m-%d')):
        self.saved_path = saved_path
        self.mail_category = mail_category
        self.check_date_sql = check_date
        self.main()

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

    def ON_HAND_ETL(self):
        eu = ETL_Utils(excel_name = 'ON_HAND')
        eu.meta_sheet_info()
        eu.read_excel()
        eu.clean_data_text(eu.data_text)
        eu.clean_data_int(int)
        eu.clean_data_float(float)
        eu.clean_data_datetime(eu.yyyymmdd_datetime)
        return eu.error_check

    def ORDER_STATUS_REPORT_ETL(self):
        eu = ETL_Utils(excel_name = 'ORDER_STATUS_REPORT')
        eu.meta_sheet_info()
        eu.read_excel()
        eu.clean_data_text(eu.data_text)
        eu.clean_data_int(eu.data_int)
        eu.clean_data_int(int)
        eu.clean_data_float(eu.data_float)
        eu.clean_data_float(float)
        eu.clean_data_datetime(eu.yyyymmdd_datetime)
        return eu.error_check
        
    def OUT_bound_ETL(self):
        eu = ETL_Utils(excel_name = 'OUT_BOUND')
        eu.meta_sheet_info()
        eu.read_excel()
        eu.clean_data_text(eu.data_text)
        eu.clean_data_int(int)
        eu.clean_data_float(float)
        eu.clean_data_datetime(eu.yyyymmdd_datetime)
        return eu.error_check
    
    def IN_BOUND_ETL(self):
        eu = ETL_Utils(excel_name = 'IN_BOUND')
        eu.meta_sheet_info()
        eu.read_excel()
        eu.clean_data_text(eu.data_text)
        eu.clean_data_int(int)
        eu.clean_data_float(float)
        eu.clean_data_datetime(eu.yyyymmdd_datetime)
        return eu.error_check

    def ETL_error_check(self):
        eu = Email_Utils()
        eu.connect_azuredb()
        
        if len(self.error_check) > 1:
            print('[WARNING] DURING ETL ERROR OCCURRED. WARNING MAIL SENDING')
            sql = f'''select MailAddr from ADMIN_INFO WHERE RnR = 'Dev' '''
            checkisadmin = eu.fetch_data(sql)

            for mailaddr in checkisadmin['MailAddr'].tolist():
                eu.send_email('[ERROR] {} ETL ERROR MESSAGE'.format(self.mail_category) 
                                ,'[MESSAGE TYPE] ERROR INFORMING DURING ETL PROCESSING'
                                , '<br>'.join(self.error_check) 
                                , destination =  mailaddr)
                break
        eu.conn.close()
        
    def main(self):
        print('[EVENT] DATA ETL PIPELINE JUST BEGAN : {} !'.format(self.mail_category))
        if self.mail_category != 'ALL':
            self.Check_file_is_updated()
        if self.mail_category == 'IN_BOUND':
            self.error_check = self.IN_BOUND_ETL()
        if self.mail_category == 'OUT_BOUND':
            self.error_check = self.OUT_bound_ETL()
        if self.mail_category == 'ON_HAND':
            self.error_check = self.ON_HAND_ETL()
        if self.mail_category == 'ORDER_STATUS_REPORT':
            self.error_check = self.ORDER_STATUS_REPORT_ETL()
        self.ETL_error_check()

class Email_Utils(Master_Reset):
    def __init__(self, mail_receivers =  "digital_scm@us.q-cells.com"):
        super().__init__()
        self.mail_receivers = mail_receivers
        self.outlook = win32com.client.Dispatch("Outlook.Application")
        self.Rxoutlook = self.outlook.GetNamespace("MAPI")
        self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)
        self.similarity_threshold = 0.5

    def send_email(self, mail_title, content_title, content_body, destination, attachment_path=''):    
        with open("../utils/template.html", "r", encoding='utf-8') as f:
            text= f.read()
        text = text.replace('RPA-TITLE' , content_title)        
        text = text.replace('RPA-CONTENTS', content_body)        
        Txoutlook = self.outlook.CreateItem(0)
        Txoutlook.To = destination
        Txoutlook.Subject = mail_title
        Txoutlook.HTMLBody = f"""{text}"""
        if attachment_path:
            Txoutlook.Attachments.Add(attachment_path)
        Txoutlook.Send()    
        print('[EVENT] SENT MAIL TO {}'.format(destination))

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
            print("[WARNING] OUTLOOK APP WAS RESTARTED. TRYING TO RE-CONNECT")
            self.outlook = win32com.client.Dispatch("Outlook.Application")
            self.Rxoutlook = self.outlook.GetNamespace("MAPI")    
            self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)  
            self.inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
            pass

    def category_replace(self):
        if self.mail_category == 'outbound':
                self.mail_category = 'OUT_BOUND'
        elif self.mail_category == 'inbound':
            self.mail_category = 'IN_BOUND'
        elif self.mail_category == 'onhand':
            self.mail_category = 'ON_HAND'
        elif self.mail_category == 'orderstatusreport':
            self.mail_category = 'ORDER_STATUS_REPORT'
        else:
            raise ValueError('THERE IS NO CATEGORY IN THE LIST')

    def invalidate_whitelist(self, mail_title, mail_domain):
        self.connect_azuredb()
        sql = 'select Mailtitle, Domain from MAIL_LIST ml'
        whitelist_dict = self.fetch_data(sql)
        whitelist_dict = whitelist_dict.to_dict('records')
        mail_domain = mail_domain.split('@')[-1]
        similarity = 0
        mail_category = 'None'
        for idx, whitelist in enumerate(whitelist_dict):
            if similarity <= SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio():
                similarity = SequenceMatcher(None, whitelist['Mailtitle'], mail_title).ratio()
                max_ssim_title = whitelist['Mailtitle']
                max_ssim_domian = whitelist_dict[idx]['Domain']                
                if (mail_domain == max_ssim_domian) & (similarity > self.similarity_threshold):
                    isdomainsame = True
                    mail_category = self.get_mailcategory(max_ssim_title)
                    break
                else:
                    isdomainsame = False
        return similarity, isdomainsame, mail_category
    
    def get_mailcategory(self, max_ssim_title):
        sql = f'''
        select distinct MailCategory from MAIL_LIST
        WHERE MailTitle = '{max_ssim_title}'
        '''
        return self.fetch_data(sql).values[0][0]

    def save_attachments(self, att, i, save_date = datetime.now().strftime('%Y-%m-%d')):
        print(att.FileName) #attachment name    
        print('[EVENT] SAVED ATTACHMENTS')
        saved_dir = Global.root_path + '/data/' +i.SentOn.strftime('%Y-%m-%d')
        saved_path = saved_dir + '/' + att.FileName
        os.makedirs(saved_dir, exist_ok = True)
        att.SaveAsFile(saved_path)
        ETL_Pipelines(saved_path, self.mail_category, save_date)

    def write_logs(self, FileName, Result, Received_time):
        print('[EVENT] SAVED ATTACHMENTS LOG WROTTEN IN RPA_DOWNLOAD_LOGS')
        self.connect_azuredb()
        sql = f'''INSERT INTO RPA_DOWNLOAD_LOGS (ExcelName, Result, MailReceivedDate)
                VALUES('{FileName}', '{Result}', '{Received_time}');'''
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    # def recevie_email(self, check_sd, download_filetype, saveYN):
    #     #TDD download email data, request master, request data(previous, today), reset master, reset data(previous, today) 
    #     try:
    #         inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
    #     except:
    #         print("[WARNING] OUTLOOK APP WAS RESTARTED. TRYING TO RE-CONNECT")
    #         self.outlook = win32com.client.Dispatch("Outlook.Application")
    #         self.Rxoutlook = self.outlook.GetNamespace("MAPI")    
    #         self.recip = self.Rxoutlook.CreateRecipient(self.mail_receivers)  
    #         inbox = self.Rxoutlook.GetSharedDefaultFolder(self.recip, 6)
    #         pass

    #     print('[ITERATION] TOTAL E-MAIL FILED: {}'.format(len(inbox.items)))
    #     for i in inbox.items: #inbox mail iteration
    #         atts = []
    #         # try:
    #         if datetime.strptime(i.SentOn.strftime('%Y-%m-%d'), '%Y-%m-%d') >= datetime.strptime(check_sd, '%Y-%m-%d'): #YYYYMMDD previous mail filtering out
    #             for filetype in range(len(download_filetype)):
    #                 atts.append([att for att in i.Attachments if download_filetype[filetype] in att.FileName.split('.')[-1].lower()]) #specific extension filtering
    #             atts = list(itertools.chain(*atts))
    #             sender_addr = self.sender_mailaddr_extract(i)
    #             if i.subject.lower().strip() == 'request master':
    #                 print('[EVENT] RECEIVED REQUEST MASTER XLSX ATTACHMENTS')
    #                 self.send_email('[RPA] MASTER FILE SHARING', 'MASTER FILE REQUEST'
    #                                 ,'RETURNING MASTER EXCEL FILE'
    #                                 ,destination = self.sender_mailaddr_extract(i) 
    #                                 ,attachment_path = Global.root_path + '/data/master.xlsx')
    #                 i.Delete()
    #                 continue

    #             elif i.subject.lower().split('/')[0].strip() == 'request data':
    #                 try:
    #                     mail_category = re.sub(r'[^a-zA-Z]', '', i.subject.lower().split('/')[1])                    
    #                     if len(i.subject.lower().split('/')) >= 3:
    #                         etl_util = ETL_Utils('')
    #                         Req_date = etl_util.yyyymmdd_datetime(i.subject.lower().split('/')[2])
    #                     else:
    #                         Req_date = datetime.now().strftime('%Y-%m-%d')
    #                     Req_date_sql = f''' WHERE Updated_Date = '{Req_date}' '''
                        
    #                     if mail_category == 'outbound':
    #                         mail_category = 'OUT_BOUND'
    #                     elif mail_category == 'inbound':
    #                         mail_category = 'IN_BOUND'
    #                     elif mail_category == 'onhand':
    #                         mail_category = 'ON_HAND'
    #                     elif mail_category == 'orderstatusreport':
    #                         mail_category = 'ORDER_STATUS_REPORT'
    #                     else:
    #                         raise ValueError('THERE IS NO CATEGORY IN THE LIST')
                    
    #                     sql = f'''SELECT * FROM {mail_category}''' + Req_date_sql
    #                     self.connect_azuredb()
    #                     df = self.fetch_data(sql)

    #                     if len(df) > 0: 
    #                         df.to_csv(Global.root_path + '/data/{}.csv'.format(mail_category), index=False)
    #                         print('[EVENT] RECEIVED REQUEST {} {} XLSX ATTACHMENTS'.format(mail_category, Req_date))
    #                         self.send_email('[RPA] {} FILE SHARING'.format(mail_category)
    #                                         ,'{} FILE REQUEST'.format(mail_category)
    #                                         ,'RETURNING {} EXCEL FILE'.format(mail_category)
    #                                         ,destination = self.sender_mailaddr_extract(i)
    #                                         ,attachment_path=Global.root_path + '/data/{}.csv'.format(mail_category))
    #                     else:
    #                         print('[EVENT] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT NO UPDATED DATA'.format(mail_category))
    #                         self.send_email('[RPA] {} FILE SHARING'.format(mail_category)
    #                                         ,'{} FILE REQUEST'.format(mail_category)
    #                                         ,'THERE IS NO UPDATED FILE : {}'.format(mail_category)
    #                                         ,destination = self.sender_mailaddr_extract(i))
    #                     i.Delete()
    #                     continue
    #                 except:
    #                         print('[ERROR] RECEIVED REQUEST {} XLSX ATTACHMENTS, BUT ERROR OCCURRED'.format(mail_category))
    #                         self.send_email('[ERROR] {} FILE SHARING'.format(mail_category)
    #                                         ,'{} FILE REQUEST'.format(mail_category)
    #                                         , 'RPA WOULD LIKE TO RECIVE THIS FORMAT <br> EX) request data/inbound/20220101 <br> PLEASE CHECK OUT MAIL TITLE FORMAT'
    #                                         ,destination = self.sender_mailaddr_extract(i))
    #                         pass
    #             else:
    #                 if len(atts) > 0: #attachment over 1
    #                     similarity, isdomainsame, self.mail_category = self.invalidate_whitelist(i.subject, sender_addr)
                        
    #                     if i.subject.lower().split('/')[0].strip() == 'update data':
    #                         # 날짜 추출을 dataframe의 update_date로 변경 // excel file encrypt error 처리 
    #                         # 현재 request -> data save -> etl pipeline -> delete prev -> insert에서 updated_date column을 포함하지 않고 insert함.
    #                         mail_category = re.sub(r'[^a-zA-Z]', '', i.subject.lower().split('/')[1])  
    #                         if len(i.subject.lower().split('/')) >= 3:
    #                             etl_util = ETL_Utils('')
    #                             Req_date = etl_util.yyyymmdd_datetime(i.subject.lower().split('/')[2])
    #                         else:
    #                             Req_date = datetime.now().strftime('%Y-%m-%d')
                            
    #                         if mail_category == 'outbound':
    #                             self.mail_category = 'OUT_BOUND'
    #                         elif mail_category == 'inbound':
    #                             self.mail_category = 'IN_BOUND'
    #                         elif mail_category == 'onhand':
    #                             self.mail_category = 'ON_HAND'
    #                         elif mail_category == 'orderstatusreport':
    #                             self.mail_category = 'ORDER_STATUS_REPORT'
    #                         else:
    #                             raise ValueError('THERE IS NO CATEGORY IN THE LIST')
                        
    #                         self.check_isadmin(sender_addr)
    #                         if self.checkisadmin.values[0][0] == 1:
    #                             print('\n' + 'Manual Update ' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
    #                             print(i.subject) # mail title
    #                             print(i.Sender, sender_addr, i.CC) #mail sender
    #                             for att in atts:
    #                                 self.save_attachments(att, i, Req_date)
    #                                 self.write_logs(att.FileName, 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
                                
    #                             i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
                                
    #                             #check if updated -> delete data -> insert query
    #                             continue

    #                     elif i.subject.lower() == 'reset master':
    #                         print('[EVENT] RECEIVED RESET MASTER XLSX ATTACHMENTS')
    #                         self.check_isadmin(sender_addr)
    #                         if self.checkisadmin.values[0][0] == 1:
    #                             for att in atts:
    #                                 if att.FileName == 'master.xlsx':
    #                                     att.SaveAsFile(Global.root_path + '/data/' + att.FileName) # saving Master file                                        
    #                                     try:
    #                                         self.master_reset_main(sender_addr)
    #                                         os.makedirs(Global.root_path + '/data/MASTER_HIST', exist_ok = True)
    #                                         att.SaveAsFile(Global.root_path + '/data/MASTER_HIST/' + i.SentOn.strftime('%Y%m%d%H%M%S') + '_' + att.FileName) #saving Backup file
    #                                         self.send_email('[RPA] MASTER FILE RESET RESULT', 'MASTER RESET'
    #                                                         , 'RPA SYSTEM USED THIS FILE. YOUR REQUEST SUCCESSFULLY APLLIED <br> SENDING YOU THE NEWEST MASTER FILE'
    #                                                         ,destination = self.sender_mailaddr_extract(i)
    #                                                         ,attachment_path = Global.root_path + '/data/master.xlsx')
    #                                         self.write_logs('MASTER', 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
    #                                     except Exception as e:
    #                                         print('[WARNING] MASTER FILE IS DAMAGED. RPA WILL ROLL-BACK.' , '\n', str(e))
    #                                         self.master_reset_main(sender_addr, file_path = glob.glob(Global.root_path + '/data/MASTER_HIST/*.xlsx')[-1])
    #                                         self.send_email('[ERROR] MASTER FILE RESET RESULT', 'MASTER RESET ERROR'
    #                                                         ,'MASTER FILE YOU WOULD LIKE TO RESET SEEMS DAMAGED. <br> PLEASE CHECK OUT THE FILE AGAIN'
    #                                                         ,destination = self.sender_mailaddr_extract(i))
    #                         else:
    #                             print('[WARNING] send e-mail that rejected due to low-authorization')
    #                             self.send_email('[WARNING] MASTER FILE RESET RESULT', 'MASTER RESET DENIED'
    #                                             ,'YOUR MAIL ADDRESS IS NOT AUTHORIZED. PLEASE RESISTER YOUR MAIL AS A MANAGER'
    #                                             ,destination = self.sender_mailaddr_extract(i))
    #                         i.Delete()  
    #                         continue

    #                     elif (similarity > self.similarity_threshold) & (isdomainsame == True): #title ssim over 0.9, domain filtering
    #                         print('\n' + '=' * 10, i.SentOn.strftime('%Y-%m-%d'), '=' * 10)
    #                         print(i.subject) # mail title
    #                         print(i.Sender, sender_addr, i.CC) #mail sender
    #                         for att in atts:
    #                             if saveYN == True:
    #                                 self.save_attachments(att, i)
    #                                 self.write_logs(att.FileName, 'PASS', i.SentOn.strftime('%Y-%m-%d %H:%M:%S'))
    #                         i.Move([i for i in self.Rxoutlook.Folders if str(i) == 'ATP_ATTACHMENTS'][0])
    #                         continue                    
    #         else:
    #             i.Delete()
    #         # except Exception as e:
    #         #     print(e)
    #         #     i.Delete()
    #         #     pass 
    #     print('[ITERATION] INBOX CHECKING JUST DONE')
            

